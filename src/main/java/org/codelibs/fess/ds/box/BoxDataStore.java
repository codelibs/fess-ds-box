/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.box;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.curl.Curl;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class BoxDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(BoxDataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    protected static final String FIELDS = "fields";
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    // scripts
    protected static final String FILE = "file";
    // original
    protected static final String FILE_URL = "url";
    protected static final String FILE_CONTENTS = "contents";
    protected static final String FILE_MIMETYPE = "mimetype";
    protected static final String FILE_FILETYPE = "filetype";
    // default
    protected static final String FILE_TYPE = "type";
    protected static final String FILE_ID = "id";
    protected static final String FILE_FILE_VERSION = "file_version";
    protected static final String FILE_SEQUENCE_ID = "sequence_id";
    protected static final String FILE_ETAG = "etag";
    protected static final String FILE_SHA1 = "sha1";
    protected static final String FILE_NAME = "name";
    protected static final String FILE_DESCRIPTION = "description";
    protected static final String FILE_SIZE = "size";
    protected static final String FILE_PATH_COLLECTION = "path_collection";
    protected static final String FILE_CREATED_AT = "created_at";
    protected static final String FILE_MODIFIED_AT = "modified_at";
    protected static final String FILE_TRASHED_AT = "trashed_at";
    protected static final String FILE_PURGED_AT = "purged_at";
    protected static final String FILE_CONTENT_CREATED_AT = "content_created_at";
    protected static final String FILE_CONTENT_MODIFIED_AT = "content_modified_at";
    protected static final String FILE_CREATED_BY = "created_by";
    protected static final String FILE_MODIFIED_BY = "modified_by";
    protected static final String FILE_OWNED_BY = "owned_by";
    protected static final String FILE_SHARED_LINK = "shared_link";
    protected static final String FILE_PARENT = "parent";
    protected static final String FILE_ITEM_STATUS = "item_status";
    protected static final String FILE_VERSION_NUMBER = "version_number";
    protected static final String FILE_COMMENT_COUNT = "comment_count";
    protected static final String FILE_PERMISSIONS = "permissions";
    protected static final String FILE_TAGS = "tags";
    protected static final String FILE_LOCK = "lock";
    protected static final String FILE_EXTENSION = "extension";
    protected static final String FILE_IS_PACKAGE = "is_package";
    // other
    protected static final String FILE_DOWNLOAD_URL = "download_url";
    protected static final String FILE_IS_WATERMARK = "is_watermark";
    protected static final String FILE_METADATA = "metadata";
    protected static final String FILE_COLLECTIONS = "collections";
    protected static final String FILE_REPRESENTATIONS = "representations";

    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return "Box";
    }

    @Override
    public void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("config: {}", config);
        }
        final ExecutorService executorService =
                Executors.newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));

        try (final BoxClient client = createClient(paramMap)) {
            crawlUserFolders(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, executorService, client);
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Interrupted.", e);
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected void crawlUserFolders(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final BoxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("crawling user folders.");
        }
        client.getUsers(user -> {
            final BoxFolder folder = client.getRootFolder(user.getID());
            client.getFiles(folder, user.getID(), config.fields, file -> executorService
                    .execute(() -> storeFile(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client, file)));
        });
    }

    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final BoxClient client, final BoxFile file) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            final BoxFile.Info info = file.getInfo();
            final String mimeType = getFileMimeType(file);
            if (Stream.of(config.supportedMimeTypes).noneMatch(mimeType::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", mimeType);
                }
                return;
            }

            final String url = getUrl(client, info);
            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            logger.info("Crawling URL: {}", url);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> fileMap = new HashMap<>();

            if (info.getSize() > config.maxSize) {
                throw new MaxLengthExceededException(
                        "The content length (" + info.getSize() + " byte) is over " + config.maxSize + " byte. The url is " + url);
            }

            final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);

            fileMap.put(FILE_URL, url);
            fileMap.put(FILE_CONTENTS, getFileContents(client, file, mimeType, config.ignoreError));
            fileMap.put(FILE_MIMETYPE, mimeType);
            fileMap.put(FILE_FILETYPE, fileType);

            fileMap.put(FILE_TYPE, info.getType());
            fileMap.put(FILE_ID, info.getID());
            fileMap.put(FILE_FILE_VERSION, info.getVersion()); //
            fileMap.put(FILE_SEQUENCE_ID, info.getSequenceID());
            fileMap.put(FILE_ETAG, info.getEtag());
            fileMap.put(FILE_SHA1, info.getSha1());
            fileMap.put(FILE_NAME, info.getName());
            fileMap.put(FILE_DESCRIPTION, info.getDescription());
            fileMap.put(FILE_SIZE, info.getSize());
            fileMap.put(FILE_PATH_COLLECTION, info.getPathCollection()); //
            fileMap.put(FILE_CREATED_AT, info.getCreatedAt());
            fileMap.put(FILE_MODIFIED_AT, info.getModifiedAt());
            fileMap.put(FILE_TRASHED_AT, info.getTrashedAt());
            fileMap.put(FILE_PURGED_AT, info.getPurgedAt());
            fileMap.put(FILE_CONTENT_CREATED_AT, info.getContentCreatedAt());
            fileMap.put(FILE_CONTENT_MODIFIED_AT, info.getContentModifiedAt());
            fileMap.put(FILE_CREATED_BY, info.getCreatedBy()); //
            fileMap.put(FILE_MODIFIED_BY, info.getModifiedBy()); //
            fileMap.put(FILE_OWNED_BY, info.getOwnedBy()); //
            fileMap.put(FILE_SHARED_LINK, info.getSharedLink()); //
            fileMap.put(FILE_PARENT, info.getParent()); //
            fileMap.put(FILE_ITEM_STATUS, info.getItemStatus());
            fileMap.put(FILE_VERSION_NUMBER, info.getVersionNumber());
            fileMap.put(FILE_COMMENT_COUNT, info.getCommentCount());
            fileMap.put(FILE_PERMISSIONS, info.getPermissions()); //
            fileMap.put(FILE_TAGS, info.getTags()); //
            fileMap.put(FILE_LOCK, info.getLock()); //
            fileMap.put(FILE_EXTENSION, info.getExtension());
            fileMap.put(FILE_IS_PACKAGE, info.getIsPackage());

            fileMap.put(FILE_DOWNLOAD_URL, file.getDownloadURL());
            /*
            fileMap.put(FILE_IS_WATERMARK, info.getIsWatermarked());
            fileMap.put(FILE_METADATA, file.getMetadata()); //
            fileMap.put(FILE_COLLECTIONS, info.getCollections()); //
            fileMap.put(FILE_REPRESENTATIONS, info.getRepresentations()); //
            */

            resultMap.put(FILE, fileMap);
            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, "", target);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
        }
    }

    protected String getFileContents(final BoxClient client, final BoxFile file, final String mimeType, final boolean ignoreError) {
        // TODO .boxnote
        final BoxFile.Info info = file.getInfo();
        try (final InputStream in = client.getFileInputStream(file)) {
            Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(mimeType);
            if (extractor == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("use a default extractor as {} by {}", extractorName, mimeType);
                }
                extractor = ComponentUtil.getComponent(extractorName);
            }
            return extractor.getText(in, null).getContent();
        } catch (final Exception e) {
            if (ignoreError) {
                logger.warn("Failed to get contents: " + info.getName(), e);
                return StringUtil.EMPTY;
            } else {
                throw new DataStoreCrawlingException(file.getPreviewLink().toString(), "Failed to get contents: " + info.getName(), e);
            }
        }
    }

    protected String getFileMimeType(final BoxFile file) {
        return Curl.head(file.getDownloadURL().toString()).execute().getHeaderValue("content-type");
    }

    protected String getUrl(final BoxClient client, final BoxItem.Info info) {
        return client.getBaseUrl() + "/" + info.getType() + "/" + info.getID();
    }

    protected BoxClient createClient(final Map<String, String> paramMap) {
        return new BoxClient(paramMap);
    }

    protected static class Config {
        final String[] fields;
        final long maxSize;
        final boolean ignoreFolder, ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        Config(final Map<String, String> paramMap) {
            fields = getFields(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreFolder = isIgnoreFolder(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
            // urlFilter = null;
        }

        private String[] getFields(final Map<String, String> paramMap) {
            final String value = paramMap.get(FIELDS);
            if (value != null) {
                return StreamUtil.split(value, ",").get(stream -> stream.map(String::trim).toArray(String[]::new));
            }
            return null;
        }

        private long getMaxSize(final Map<String, String> paramMap) {
            final String value = paramMap.get(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreFolder(final Map<String, String> paramMap) {
            return paramMap.getOrDefault(IGNORE_FOLDER, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
        }

        private boolean isIgnoreError(final Map<String, String> paramMap) {
            return paramMap.getOrDefault(IGNORE_ERROR, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
        }

        private String[] getSupportedMimeTypes(final Map<String, String> paramMap) {
            return StreamUtil.split(paramMap.getOrDefault(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final Map<String, String> paramMap) {
            final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            final String include = paramMap.get(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.get(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.get(Constants.CRAWLING_INFO_ID));
            if (logger.isDebugEnabled()) {
                logger.debug("urlFilter: {}", urlFilter);
            }
            return urlFilter;
        }

        @Override
        public String toString() {
            return "{maxSize=" + maxSize + "ignoreError=" + ignoreError + "ignoreFolder=" + ignoreFolder + "supportedMimeTypes=" + Arrays
                    .toString(supportedMimeTypes) + "urlFilter=" + urlFilter + "}";
        }
    }

}

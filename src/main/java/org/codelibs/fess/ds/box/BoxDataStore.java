/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.io.ResourceUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.curl.Curl;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator.Info;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    protected static final String FILTER_TERM = "filter_term";

    // scripts
    protected static final String FILE = "file";
    // original
    protected static final String FILE_URL = "url";
    protected static final String FILE_CONTENTS = "contents";
    protected static final String FILE_MIMETYPE = "mimetype";
    protected static final String FILE_FILETYPE = "filetype";
    protected static final String FILE_DOWNLOAD_URL = "download_url";
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
    public void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Config config = new Config(paramMap);
        if (logger.isDebugEnabled()) {
            logger.debug("box config: {}", config);
        }

        try (final BoxClient client = createClient(paramMap)) {
            crawlUserFolders(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client);
        }
    }

    protected void crawlUserFolders(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final BoxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("crawling user folders.");
        }
        final int numOfThread = Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        final String filterTerm = paramMap.getAsString(FILTER_TERM);
        client.asSelf();
        client.getUsers(filterTerm, info -> {
            final BoxUser user = info.getResource();
            final String userId = user.getID();
            if (logger.isDebugEnabled()) {
                logger.debug("crawling by {}", userId);
            }
            client.asUser(userId);
            final BoxFolder folder = client.getRootFolder();
            final ExecutorService executorService = newFixedThreadPool(numOfThread);
            try {
                client.getFiles(folder, userId, config.fields, file -> executorService
                        .execute(() -> storeFile(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client, file)));
                if (logger.isDebugEnabled()) {
                    logger.debug("shutting down executor..");
                }
                executorService.shutdown();
                executorService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } finally {
                executorService.shutdownNow();
            }
            client.asSelf();
        });
    }

    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final BoxClient client, final BoxFile file) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(file.getID());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            final BoxFile.Info info = file.getInfo();
            final String downloadURL = file.getDownloadURL().toExternalForm();
            if (logger.isDebugEnabled()) {
                logger.debug("downloadURL: {}", downloadURL);
                logger.debug("info: {}", info.getJson());
            }
            final String mimeType = getFileMimeType(downloadURL);
            if (Stream.of(config.supportedMimeTypes).noneMatch(mimeType::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", mimeType);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String path = getPath(info);
            if (logger.isDebugEnabled()) {
                logger.debug("path: {}", path);
            }
            final UrlFilter urlFilter = config.urlFilter;
            if (urlFilter != null && !urlFilter.match(path)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", path);
                }
                return;
            }

            final String url = getUrl(client, info);
            logger.info("Crawling URL: {}", url);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> fileMap = new HashMap<>();

            if (info.getSize() > config.maxSize) {
                throw new MaxLengthExceededException(
                        "The content length (" + info.getSize() + " byte) is over " + config.maxSize + " byte. The url is " + url);
            }

            final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);

            fileMap.put(FILE_URL, url);
            fileMap.put(FILE_CONTENTS, getFileContents(client, file, info, downloadURL, mimeType, config.ignoreError));
            fileMap.put(FILE_MIMETYPE, mimeType);
            fileMap.put(FILE_FILETYPE, fileType);
            fileMap.put(FILE_DOWNLOAD_URL, downloadURL);
            fileMap.put(FILE_TYPE, info.getType());
            fileMap.put(FILE_ID, info.getID());
            fileMap.put(FILE_FILE_VERSION, info.getVersion());
            fileMap.put(FILE_SEQUENCE_ID, info.getSequenceID());
            fileMap.put(FILE_ETAG, info.getEtag());
            fileMap.put(FILE_SHA1, info.getSha1());
            fileMap.put(FILE_NAME, info.getName());
            fileMap.put(FILE_DESCRIPTION, info.getDescription());
            fileMap.put(FILE_SIZE, info.getSize());
            fileMap.put(FILE_PATH_COLLECTION, info.getPathCollection());
            fileMap.put(FILE_CREATED_AT, info.getCreatedAt());
            fileMap.put(FILE_MODIFIED_AT, info.getModifiedAt());
            fileMap.put(FILE_TRASHED_AT, info.getTrashedAt());
            fileMap.put(FILE_PURGED_AT, info.getPurgedAt());
            fileMap.put(FILE_CONTENT_CREATED_AT, info.getContentCreatedAt());
            fileMap.put(FILE_CONTENT_MODIFIED_AT, info.getContentModifiedAt());
            fileMap.put(FILE_CREATED_BY, info.getCreatedBy());
            fileMap.put(FILE_MODIFIED_BY, info.getModifiedBy());
            fileMap.put(FILE_OWNED_BY, info.getOwnedBy());
            fileMap.put(FILE_SHARED_LINK, info.getSharedLink());
            fileMap.put(FILE_PARENT, info.getParent());
            fileMap.put(FILE_ITEM_STATUS, info.getItemStatus());
            fileMap.put(FILE_VERSION_NUMBER, info.getVersionNumber());
            fileMap.put(FILE_COMMENT_COUNT, info.getCommentCount());
            fileMap.put(FILE_PERMISSIONS, info.getPermissions());
            fileMap.put(FILE_TAGS, info.getTags());
            fileMap.put(FILE_LOCK, info.getLock());
            fileMap.put(FILE_EXTENSION, info.getExtension());
            fileMap.put(FILE_IS_PACKAGE, info.getIsPackage());

            fileMap.put(FILE_IS_WATERMARK, info.getIsWatermarked());
            // fileMap.put(FILE_METADATA, file.getMetadata());
            fileMap.put(FILE_COLLECTIONS, info.getCollections());
            fileMap.put(FILE_REPRESENTATIONS, info.getRepresentations());

            fileMap.put("api", new BoxFileAPI(file));

            resultMap.put(FILE, fileMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

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
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    protected String getFileContents(final BoxClient client, final BoxFile file, final BoxFile.Info info, final String downloadURL,
            final String mimeType, final boolean ignoreError) {
        final String name = info.getName();
        try (final InputStream in = client.getFileInputStream(file)) {
            if ("boxnote".equals(ResourceUtil.getExtension(name))) {
                return getBoxNoteContents(in);
            }
            return ComponentUtil.getExtractorFactory().builder(in, null).mimeType(mimeType).extractorName(extractorName).extract()
                    .getContent();
        } catch (final Exception e) {
            if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(downloadURL, "Failed to get contents: " + name, e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents: {}", name, e);
            } else {
                logger.warn("Failed to get contents: {}. {}", name, e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }

    protected String getBoxNoteContents(final InputStream in) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode node = mapper.readTree(in);
        return node.get("atext").get("text").asText();
    }

    protected String getFileMimeType(final String downloadURL) {
        return Curl.head(downloadURL).execute().getHeaderValue("content-type");
    }

    protected String getPath(final BoxItem.Info info) {
        return info.getPathCollection().stream().map(BoxItem.Info::getName).collect(Collectors.joining("/"));
    }

    protected String getUrl(final BoxClient client, final BoxItem.Info info) {
        return client.getBaseUrl() + "/" + info.getType() + "/" + info.getID();
    }

    protected BoxClient createClient(final DataStoreParams paramMap) {
        final BoxClient client = new BoxClient();
        client.setInitParameterMap(paramMap.asMap());
        client.init();
        return client;
    }

    protected static class Config {
        final String[] fields;
        final long maxSize;
        final boolean ignoreFolder;
        final boolean ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;

        Config(final DataStoreParams paramMap) {
            fields = getFields(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreFolder = isIgnoreFolder(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
        }

        private String[] getFields(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(FIELDS);
            if (value != null) {
                return StreamUtil.split(value, ",").get(stream -> stream.map(String::trim).toArray(String[]::new));
            }
            return null;
        }

        private long getMaxSize(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(MAX_SIZE);
            try {
                return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
            } catch (final NumberFormatException e) {
                return DEFAULT_MAX_SIZE;
            }
        }

        private boolean isIgnoreFolder(final DataStoreParams paramMap) {
            return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
        }

        private boolean isIgnoreError(final DataStoreParams paramMap) {
            return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
        }

        private String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
            return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                    .get(stream -> stream.map(String::trim).toArray(String[]::new));
        }

        private UrlFilter getUrlFilter(final DataStoreParams paramMap) {
            final UrlFilter urlFilter;
            try {
                urlFilter = ComponentUtil.getComponent(UrlFilter.class);
            } catch (final ComponentNotFoundException e) {
                return null;
            }
            final String include = paramMap.getAsString(INCLUDE_PATTERN);
            if (StringUtil.isNotBlank(include)) {
                urlFilter.addInclude(include);
            }
            final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
            if (StringUtil.isNotBlank(exclude)) {
                urlFilter.addExclude(exclude);
            }
            urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
            if (logger.isDebugEnabled()) {
                logger.debug("urlFilter: {}", urlFilter);
            }
            return urlFilter;
        }

        @Override
        public String toString() {
            return "{fields=" + Arrays.toString(fields) + ",maxSize=" + maxSize + ",ignoreError=" + ignoreError + ",ignoreFolder="
                    + ignoreFolder + ",supportedMimeTypes=" + Arrays.toString(supportedMimeTypes) + ",urlFilter=" + urlFilter + "}";
        }
    }

    public static class BoxFileAPI {

        private final BoxFile file;

        private Iterable<BoxCollaboration.Info> collaborations;

        public BoxFileAPI(final BoxFile file) {
            this.file = file;
            if (logger.isDebugEnabled()) {
                loadCollaborations(file);
            }
        }

        private void loadCollaborations(final BoxFile file) {
            collaborations = file.getAllFileCollaborations();
            if (collaborations == null) {
                collaborations = Collections.emptyList();
                if (logger.isDebugEnabled()) {
                    logger.debug("no collaborations");
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("collaboration: {}",
                        StreamSupport.stream(collaborations.spliterator(), false).map(c -> c.getJson()).collect(Collectors.joining(",")));
            }
        }

        public List<BoxCollaboration.Info> getAllFileCollaborations() {
            if (collaborations == null) {
                loadCollaborations(file);
            }
            return StreamSupport.stream(collaborations.spliterator(), false).collect(Collectors.toList());
        }

        public List<String> getCollaborationRoles() {
            final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
            return getAllFileCollaborations().stream().map(c -> {
                Info accessibleBy = c.getAccessibleBy();
                if (accessibleBy == null) {
                    return null;
                }
                final String name = accessibleBy.getName();
                if (StringUtil.isBlank(name)) {
                    return null;
                }
                switch (accessibleBy.getType()) {
                case USER: {
                    return systemHelper.getSearchRoleByUser(name);
                }
                case GROUP: {
                    return systemHelper.getSearchRoleByGroup(name);
                }
                default:
                    return null;
                }
            }).filter(s -> s != null).collect(Collectors.toList());
        }
    }
}

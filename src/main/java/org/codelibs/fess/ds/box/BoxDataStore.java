/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.io.ResourceUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.helper.MimeTypeHelper;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.lastaflute.di.core.exception.ComponentNotFoundException;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator.Info;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;

/**
 * A data store implementation for crawling files and folders from Box.
 * This class handles the entire process of connecting to Box, fetching items,
 * extracting content, and indexing it into Fess.
 */
public class BoxDataStore extends AbstractDataStore {

    /**
     * Default constructor.
     */
    public BoxDataStore() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(BoxDataStore.class);

    /** Default maximum file size to download, in bytes (10MB). */
    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    /** Parameter key for the fields to retrieve from the Box API. */
    protected static final String FIELDS = "fields";
    /** Parameter key for the maximum file size to download. */
    protected static final String MAX_SIZE = "max_size";
    /** Parameter key to specify whether to ignore folders during crawling. */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Parameter key to specify whether to ignore errors during content extraction. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Parameter key for a comma-separated list of supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Parameter key for URL patterns to include in the crawl. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Parameter key for URL patterns to exclude from the crawl. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Parameter key for the number of threads to use for crawling. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter key for how long to wait, in seconds, for the crawl to finish after every user has been queued. */
    protected static final String THREAD_POOL_AWAIT_TIMEOUT = "thread_pool_await_timeout";
    /** Default value, in seconds, of {@link #THREAD_POOL_AWAIT_TIMEOUT}. */
    protected static final long DEFAULT_AWAIT_TIMEOUT = 60L;
    /** Parameter key for a term to filter users by. */
    protected static final String FILTER_TERM = "filter_term";
    /** Parameter key for roles applied to every document. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter key for the role granted to enterprise-wide shared links. */
    protected static final String COMPANY_SHARED_LINK_ROLE = "company_shared_link_role";
    /** Key for the file's search roles. */
    protected static final String FILE_ROLES = "roles";

    /**
     * Fields requested from the Box API. Box omits every field that is not asked
     * for once an explicit field list is supplied, so this must cover everything
     * the crawler maps.
     */
    protected static final String[] DEFAULT_FIELDS =
            { "type", "id", "etag", "sha1", "name", "description", "size", "path_collection", "created_at", "modified_at", "trashed_at",
                    "purged_at", "content_created_at", "content_modified_at", "created_by", "modified_by", "owned_by", "shared_link",
                    "parent", "item_status", "sequence_id", "file_version", "version_number", "comment_count", "permissions", "tags",
                    "lock", "extension", "is_package", "has_collaborations", "watermark_info", "collections", "representations" };

    /**
     * Parameter keys that must never reach the script evaluation context.
     */
    protected static final String[] SENSITIVE_PARAMS =
            { BoxClient.CLIENT_ID_PARAM, BoxClient.CLIENT_SECRET_PARAM, BoxClient.PUBLIC_KEY_ID_PARAM, BoxClient.PRIVATE_KEY_PARAM,
                    BoxClient.PASSPHRASE_PARAM, BoxClient.ENTERPRISE_ID_PARAM, BoxClient.PROXY_PASSWORD };

    // scripts
    /** Key for the file data map in the script context. */
    protected static final String FILE = "file";
    // original
    /** Key for the file's URL. */
    protected static final String FILE_URL = "url";
    /** Key for the file's extracted content. */
    protected static final String FILE_CONTENTS = "contents";
    /** Key for the file's MIME type. */
    protected static final String FILE_MIMETYPE = "mimetype";
    /** Key for the file's filetype (e.g., "pdf"). */
    protected static final String FILE_FILETYPE = "filetype";
    /** Key for the file's download URL. */
    protected static final String FILE_DOWNLOAD_URL = "download_url";
    // default
    /** Key for the item's type (e.g., "file", "folder"). */
    protected static final String FILE_TYPE = "type";
    /** Key for the item's ID. */
    protected static final String FILE_ID = "id";
    /** Key for the file's version information. */
    protected static final String FILE_FILE_VERSION = "file_version";
    /** Key for the item's sequence ID. */
    protected static final String FILE_SEQUENCE_ID = "sequence_id";
    /** Key for the item's ETag. */
    protected static final String FILE_ETAG = "etag";
    /** Key for the file's SHA1 hash. */
    protected static final String FILE_SHA1 = "sha1";
    /** Key for the item's name. */
    protected static final String FILE_NAME = "name";
    /** Key for the item's description. */
    protected static final String FILE_DESCRIPTION = "description";
    /** Key for the file's size in bytes. */
    protected static final String FILE_SIZE = "size";
    /** Key for the item's path collection. */
    protected static final String FILE_PATH_COLLECTION = "path_collection";
    /** Key for the item's creation timestamp. */
    protected static final String FILE_CREATED_AT = "created_at";
    /** Key for the item's modification timestamp. */
    protected static final String FILE_MODIFIED_AT = "modified_at";
    /** Key for the item's trashed timestamp. */
    protected static final String FILE_TRASHED_AT = "trashed_at";
    /** Key for the item's purged timestamp. */
    protected static final String FILE_PURGED_AT = "purged_at";
    /** Key for the file's content creation timestamp. */
    protected static final String FILE_CONTENT_CREATED_AT = "content_created_at";
    /** Key for the file's content modification timestamp. */
    protected static final String FILE_CONTENT_MODIFIED_AT = "content_modified_at";
    /** Key for the user who created the item. */
    protected static final String FILE_CREATED_BY = "created_by";
    /** Key for the user who last modified the item. */
    protected static final String FILE_MODIFIED_BY = "modified_by";
    /** Key for the user who owns the item. */
    protected static final String FILE_OWNED_BY = "owned_by";
    /** Key for the item's shared link. */
    protected static final String FILE_SHARED_LINK = "shared_link";
    /** Key for the item's parent folder. */
    protected static final String FILE_PARENT = "parent";
    /** Key for the item's status (e.g., "active"). */
    protected static final String FILE_ITEM_STATUS = "item_status";
    /** Key for the file's version number. */
    protected static final String FILE_VERSION_NUMBER = "version_number";
    /** Key for the file's comment count. */
    protected static final String FILE_COMMENT_COUNT = "comment_count";
    /** Key for the item's permissions. */
    protected static final String FILE_PERMISSIONS = "permissions";
    /** Key for the item's tags. */
    protected static final String FILE_TAGS = "tags";
    /** Key for the file's lock information. */
    protected static final String FILE_LOCK = "lock";
    /** Key for the file's extension. */
    protected static final String FILE_EXTENSION = "extension";
    /** Key indicating if the item is a package. */
    protected static final String FILE_IS_PACKAGE = "is_package";
    // other
    /** Key indicating if the file is watermarked. */
    protected static final String FILE_IS_WATERMARK = "is_watermark";
    /** Key for the item's metadata. */
    protected static final String FILE_METADATA = "metadata";
    /** Key for the item's collections. */
    protected static final String FILE_COLLECTIONS = "collections";
    /** Key for the file's representations. */
    protected static final String FILE_REPRESENTATIONS = "representations";

    /** The name of the extractor to use for file content. */
    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return "Box";
    }

    /**
     * Builds the script evaluation context from the data store parameters, with
     * credentials removed.
     *
     * @param paramMap the data store parameters
     * @return a map safe to expose to user-supplied scripts
     */
    protected Map<String, Object> createResultMap(final DataStoreParams paramMap) {
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        for (final String key : SENSITIVE_PARAMS) {
            resultMap.remove(key);
        }
        return resultMap;
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

    /**
     * Crawls the folders of each enterprise user.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index documents.
     * @param config The data store configuration.
     * @param paramMap The data store parameters.
     * @param scriptMap The script mapping.
     * @param defaultDataMap The default data map.
     * @param client The Box client.
     */
    protected void crawlUserFolders(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final BoxClient client) {
        if (logger.isDebugEnabled()) {
            logger.debug("crawling user folders.");
        }
        final String filterTerm = paramMap.getAsString(FILTER_TERM);
        final long readInterval = getReadInterval(paramMap);
        final BoxAclResolver aclResolver = new BoxAclResolver(config.defaultPermissions, config.companySharedLinkRole);
        // A single pool serves the whole crawl. The previous implementation created and
        // destroyed one pool per user, with its await applying per user, so anything still
        // queued when one user's crawl ran long was dropped by shutdownNow() without a log line.
        final ExecutorService executorService = newFixedThreadPool(getNumberOfThreads(paramMap));
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
        try {
            client.getUsers(filterTerm, info -> {
                if (!alive) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Stop requested. Skipping remaining users.");
                    }
                    return;
                }
                final BoxUser user = info.getResource();
                final String userId = user.getID();
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling by {}", userId);
                }
                // Each user gets its own connection. asUser() used to mutate the shared
                // connection while a previous user's worker threads could still be running,
                // risking a request issued under the wrong identity.
                final BoxClient userClient = client.forUser(userId);
                final BoxFolder folder = userClient.getRootFolder();
                userClient.getFiles(folder, config.fields, file -> {
                    if (!alive) {
                        return;
                    }
                    if (!markCrawled(crawledIds, file.getID())) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} was already crawled via another user.", file.getID());
                        }
                        return;
                    }
                    executorService.execute(() -> storeFile(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, userClient,
                            aclResolver, crawledIds, file));
                    if (readInterval > 0) {
                        sleep(readInterval);
                    }
                });
            });
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("shutting down executor..");
            }
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(config.awaitTimeout, TimeUnit.SECONDS)) {
                    // shutdownNow() only returns tasks that never started; it does not count
                    // files whose storeFile() task was already running and got interrupted, so
                    // this understates how many files were actually disrupted.
                    final List<Runnable> pending = executorService.shutdownNow();
                    logger.warn(
                            "The crawler did not finish within {} seconds. {} queued-but-unstarted file(s) were dropped, and any "
                                    + "file(s) still being processed were interrupted. Increase {} if this happens regularly.",
                            config.awaitTimeout, pending.size(), THREAD_POOL_AWAIT_TIMEOUT);
                }
            } catch (final InterruptedException e) {
                executorService.shutdownNow();
                throw new InterruptedRuntimeException(e);
            }
        }
    }

    /**
     * Creates a new fixed thread pool with the specified number of threads.
     *
     * @param nThreads The number of threads in the pool.
     * @return A new {@link ExecutorService}.
     */
    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Returns the thread count, clamped to a sane range.
     *
     * @param paramMap The data store parameters.
     * @return the number of crawler threads, at least 1
     */
    protected int getNumberOfThreads(final DataStoreParams paramMap) {
        final int max = Runtime.getRuntime().availableProcessors() * 2;
        int nThreads;
        try {
            nThreads = Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        } catch (final NumberFormatException e) {
            logger.warn("Invalid {}: {}. Falling back to 1.", NUMBER_OF_THREADS, paramMap.getAsString(NUMBER_OF_THREADS));
            nThreads = 1;
        }
        if (nThreads < 1) {
            return 1;
        }
        if (nThreads > max) {
            logger.info("{} is capped at {} on this machine.", NUMBER_OF_THREADS, max);
            return max;
        }
        return nThreads;
    }

    /**
     * Stores a single file in the index.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index documents.
     * @param config The data store configuration.
     * @param paramMap The data store parameters.
     * @param scriptMap The script mapping.
     * @param defaultDataMap The default data map.
     * @param client The Box client scoped to the file's user, from {@link BoxClient#forUser(String)}.
     * @param aclResolver The resolver used to compute the file's search roles.
     * @param crawledIds The set of file IDs already successfully crawled in this crawl session.
     * @param file The Box file to store.
     */
    protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final BoxClient client, final BoxAclResolver aclResolver, final Set<String> crawledIds, final BoxFile file) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(file.getID());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        final DocumentQuality quality = new DocumentQuality();
        try {
            crawlerStatsHelper.begin(statsKey);
            final BoxFile.Info info = file.getInfo(config.fields);
            final String downloadURL = file.getDownloadURL().toExternalForm();
            if (logger.isDebugEnabled()) {
                logger.debug("downloadURL: {}", downloadURL);
                logger.debug("info: {}", info.getJson());
            }
            final String mimeType = getFileMimeType(info);
            if (Stream.of(config.supportedMimeTypes).noneMatch(mimeType::matches)) {
                // application/octet-stream is the fallback when MimeTypeHelperImpl cannot
                // determine the MIME type from the filename or content
                if ("application/octet-stream".equals(mimeType)) {
                    logger.warn(
                            "The MIME type of {} could not be determined from its name, so it was "
                                    + "treated as {} and did not match supported_mimetypes. The file was not indexed.",
                            info.getName(), mimeType);
                } else if (logger.isDebugEnabled()) {
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

            final Map<String, Object> resultMap = createResultMap(paramMap);
            final Map<String, Object> fileMap = new HashMap<>();

            if (info.getSize() > config.maxSize) {
                // Over max_size is a deliberate skip, not a crawling error, so it must not
                // be reported through failureUrl: that would pollute the failure log with
                // files nobody asked to have indexed in the first place.
                logger.info("Skipping {} because its size ({} byte) is over {} ({} byte).", info.getName(), info.getSize(), MAX_SIZE,
                        config.maxSize);
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String fileType = ComponentUtil.getFileTypeHelper().get(mimeType);

            fileMap.put(FILE_URL, url);
            fileMap.put(FILE_CONTENTS, getFileContents(client, file, info, downloadURL, mimeType, config.ignoreError, quality));
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

            final List<String> baseRoles = new ArrayList<>();
            if (defaultDataMap.get(ComponentUtil.getFessConfig().getIndexFieldRole()) instanceof final List<?> roleTypeList) {
                roleTypeList.stream().map(String.class::cast).forEach(baseRoles::add);
            }
            fileMap.put(FILE_ROLES, aclResolver.getRoles(client, info, baseRoles, quality));

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
            if (quality.isDegraded()) {
                releaseCrawled(crawledIds, file.getID());
            }
        } catch (final CrawlingAccessException e) {
            releaseCrawled(crawledIds, file.getID());
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
            releaseCrawled(crawledIds, file.getID());
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), "", t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Retrieves the contents of a file.
     *
     * @param client The Box client.
     * @param file The Box file.
     * @param info The file information.
     * @param downloadURL The download URL for the file.
     * @param mimeType The MIME type of the file.
     * @param ignoreError Whether to ignore errors during content extraction.
     * @param quality optional degradation tracker; if extraction fails and the error is ignored, {@code quality.contentDegraded} is set to true
     * @return The extracted text content of the file.
     */
    protected String getFileContents(final BoxClient client, final BoxFile file, final BoxFile.Info info, final String downloadURL,
            final String mimeType, final boolean ignoreError, final DocumentQuality quality) {
        final String name = info.getName();
        try (final InputStream in = client.getFileInputStream(file)) {
            if ("boxnote".equals(ResourceUtil.getExtension(name))) {
                return getBoxNoteContents(in);
            }
            return ComponentUtil.getExtractorFactory()
                    .builder(in, null)
                    .mimeType(mimeType)
                    .extractorName(extractorName)
                    .extract()
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
            if (quality != null) {
                quality.contentDegraded = true;
            }
            return StringUtil.EMPTY;
        }
    }

    /**
     * Extracts the text content from a Box Note.
     *
     * @param in The input stream of the Box Note file.
     * @return The text content.
     * @throws IOException If an I/O error occurs.
     */
    protected String getBoxNoteContents(final InputStream in) throws IOException {
        return BoxNoteParser.parse(in);
    }

    /**
     * Resolves the MIME type of a file from its name.
     *
     * <p>This used to issue an HTTP HEAD against the download URL for every single
     * file, which doubled the request count and ignored the configured proxy.</p>
     *
     * @param info The file information.
     * @return The MIME type.
     */
    protected String getFileMimeType(final BoxFile.Info info) {
        final MimeTypeHelper mimeTypeHelper = ComponentUtil.getComponent(MimeTypeHelper.class);
        return mimeTypeHelper.getContentType(null, info.getName());
    }

    /**
     * Builds the path used for include and exclude pattern matching.
     *
     * @param ancestorNames the names of the ancestor folders, may be null
     * @param name the item name
     * @return the slash separated path, ending with the item name
     */
    static String buildPath(final List<String> ancestorNames, final String name) {
        if (ancestorNames == null || ancestorNames.isEmpty()) {
            return name;
        }
        return String.join("/", ancestorNames) + "/" + name;
    }

    /**
     * Constructs the hierarchical path of a Box item, including its own name.
     *
     * @param info The item information.
     * @return The slash-separated path.
     */
    protected String getPath(final BoxItem.Info info) {
        final List<BoxFolder.Info> pathCollection = info.getPathCollection();
        final List<String> ancestorNames =
                pathCollection == null ? List.of() : pathCollection.stream().map(BoxFolder.Info::getName).collect(Collectors.toList());
        return buildPath(ancestorNames, info.getName());
    }

    /**
     * Constructs the URL for a Box item.
     *
     * @param client The Box client.
     * @param info The item information.
     * @return The full URL to the item.
     */
    protected String getUrl(final BoxClient client, final BoxItem.Info info) {
        return client.getBaseUrl() + "/" + info.getType() + "/" + info.getID();
    }

    /**
     * Records a file as crawled and reports whether this call was the first.
     *
     * <p>Every enterprise user is walked from their own root folder, so a shared
     * file is reached once per collaborator. The resulting documents are identical
     * because the URL is derived from the file ID, but the crawl time and the API
     * calls multiply. This method optimistically claims the file ID before
     * {@link #storeFile(DataConfig, IndexUpdateCallback, Config, DataStoreParams, Map, Map, BoxClient, BoxAclResolver, Set, BoxFile)}
     * runs, allowing later collaborators to skip the file if it was already successfully indexed.</p>
     *
     * <p>Resilient to partial failures: if the first collaborator encounters an
     * error, the claim is released in two scenarios:</p>
     * <ol>
     * <li>Hard failure: storeFile throws an uncaught exception. The file stays
     * unindexed and a later collaborator can retry from scratch.</li>
     * <li>Swallowed error: content extraction fails with {@code ignore_error=true},
     * or collaboration lookup fails. The document is stored with degraded content
     * or roles, the claim is released, and a later collaborator can retry with
     * better permissions, producing a complete version that overwrites the degraded one.</li>
     * </ol>
     * <p>Best-effort: since users are walked sequentially, if all collaborators
     * have already been walked past a file when a swallowed error is discovered,
     * the degraded document remains indexed with no retry. Hard failures that
     * leave a file unindexed are unretriable if all users have been walked.</p>
     *
     * @param crawledIds the set of file IDs already seen
     * @param fileId the file ID
     * @return true if this file has not been crawled yet
     */
    protected boolean markCrawled(final Set<String> crawledIds, final String fileId) {
        return crawledIds.add(fileId);
    }

    /**
     * Releases a file from the crawled set, allowing it to be processed again.
     *
     * <p>Called in two scenarios:</p>
     * <ol>
     * <li>When {@link #storeFile(DataConfig, IndexUpdateCallback, Config, DataStoreParams, Map, Map, BoxClient, BoxAclResolver, Set, BoxFile)}
     * fails with an exception that is not caught: {@code callback.store} is never reached, and the file
     * stays unindexed. A later collaborator who reaches the same file can attempt it independently.</li>
     * <li>When the document is stored successfully but with degradation (content extraction failed with errors
     * ignored, or collaborations could not be listed): the document is indexed but incomplete. A later
     * collaborator with better permissions can retry and may produce a complete version, which overwrites
     * the degraded one (same URL derived from file ID).</li>
     * </ol>
     * <p>Releasing an id that was never claimed is harmless.</p>
     *
     * @param crawledIds the set of file IDs already seen
     * @param fileId the file ID
     */
    protected void releaseCrawled(final Set<String> crawledIds, final String fileId) {
        crawledIds.remove(fileId);
    }

    /**
     * Creates and initializes a new {@link BoxClient}.
     *
     * @param paramMap The data store parameters.
     * @return A new Box client.
     */
    protected BoxClient createClient(final DataStoreParams paramMap) {
        final BoxClient client = new BoxClient();
        client.setInitParameterMap(paramMap.asMap());
        client.init();
        return client;
    }

    /**
     * Tracks whether a document's content or ACL roles are degraded due to
     * errors that were caught and ignored rather than causing the document
     * to be skipped entirely.
     */
    protected static class DocumentQuality {
        boolean contentDegraded = false;
        boolean aclDegraded = false;

        boolean isDegraded() {
            return contentDegraded || aclDegraded;
        }
    }

    /**
     * Configuration class for the Box data store.
     * Holds all the settings required for a crawl.
     */
    protected static class Config {
        final String[] fields;
        final long maxSize;
        final boolean ignoreFolder;
        final boolean ignoreError;
        final String[] supportedMimeTypes;
        final UrlFilter urlFilter;
        final List<String> defaultPermissions;
        final String companySharedLinkRole;
        final long awaitTimeout;

        /**
         * Constructs a new Config instance from the given parameters.
         * @param paramMap The data store parameters.
         */
        Config(final DataStoreParams paramMap) {
            fields = getFields(paramMap);
            maxSize = getMaxSize(paramMap);
            ignoreFolder = isIgnoreFolder(paramMap);
            ignoreError = isIgnoreError(paramMap);
            supportedMimeTypes = getSupportedMimeTypes(paramMap);
            urlFilter = getUrlFilter(paramMap);
            defaultPermissions = getDefaultPermissions(paramMap);
            companySharedLinkRole = getCompanySharedLinkRole(paramMap);
            awaitTimeout = getAwaitTimeout(paramMap);
        }

        private String[] getFields(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(FIELDS);
            if (StringUtil.isNotBlank(value)) {
                return StreamUtil.split(value, ",").get(stream -> stream.map(String::trim).toArray(String[]::new));
            }
            return DEFAULT_FIELDS;
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

        private List<String> getDefaultPermissions(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(DEFAULT_PERMISSIONS, StringUtil.EMPTY);
            if (StringUtil.isBlank(value)) {
                return List.of();
            }
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            final List<String> list = new ArrayList<>();
            StreamUtil.split(value, ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(list::add));
            return list;
        }

        private String getCompanySharedLinkRole(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(COMPANY_SHARED_LINK_ROLE, StringUtil.EMPTY);
            if (StringUtil.isBlank(value)) {
                return StringUtil.EMPTY;
            }
            return ComponentUtil.getPermissionHelper().encode(value);
        }

        private long getAwaitTimeout(final DataStoreParams paramMap) {
            final String value = paramMap.getAsString(THREAD_POOL_AWAIT_TIMEOUT);
            long timeout;
            try {
                timeout = StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_AWAIT_TIMEOUT;
            } catch (final NumberFormatException e) {
                logger.warn("Invalid {}: {}. Falling back to {}.", THREAD_POOL_AWAIT_TIMEOUT, value, DEFAULT_AWAIT_TIMEOUT);
                timeout = DEFAULT_AWAIT_TIMEOUT;
            }
            // 0 or negative would make awaitTermination() return immediately, dropping every
            // queued file without ever giving the executor a chance to finish.
            return timeout < 1 ? 1 : timeout;
        }

        @Override
        public String toString() {
            return "{fields=" + Arrays.toString(fields) + ",maxSize=" + maxSize + ",ignoreError=" + ignoreError + ",ignoreFolder="
                    + ignoreFolder + ",supportedMimeTypes=" + Arrays.toString(supportedMimeTypes) + ",urlFilter=" + urlFilter
                    + ",defaultPermissions=" + defaultPermissions + ",companySharedLinkRole=" + companySharedLinkRole + ",awaitTimeout="
                    + awaitTimeout + "}";
        }
    }

    /**
     * A utility class to access Box File API, providing collaboration and permission information.
     */
    public static class BoxFileAPI {

        private final BoxFile file;

        private List<BoxCollaboration.Info> collaborations;

        /**
         * Constructs a new BoxFileAPI instance.
         * @param file The Box file to work with.
         */
        public BoxFileAPI(final BoxFile file) {
            this.file = file;
            if (logger.isDebugEnabled()) {
                loadCollaborations(file);
            }
        }

        private void loadCollaborations(final BoxFile file) {
            final Iterable<BoxCollaboration.Info> collaborationItr = file.getAllFileCollaborations();
            if (collaborationItr == null) {
                collaborations = Collections.emptyList();
                if (logger.isDebugEnabled()) {
                    logger.debug("no collaborations");
                }
            } else {
                collaborations = StreamSupport.stream(collaborationItr.spliterator(), false).collect(Collectors.toList());
                if (logger.isDebugEnabled()) {
                    logger.debug("collaboration: {}", collaborations.stream().map(c -> c.getJson()).collect(Collectors.joining(",")));
                }
            }
        }

        /**
         * Retrieves all collaborations for the file.
         * The result is cached after the first call.
         * @return A list of collaboration information.
         */
        public List<BoxCollaboration.Info> getAllFileCollaborations() {
            if (collaborations == null) {
                loadCollaborations(file);
            }
            return collaborations;
        }

        /**
         * Returns whether a collaboration grants read access that should be
         * reflected in the search index.
         *
         * <p>Only accepted collaborations grant access at all; pending and rejected
         * collaborators cannot open the file. The uploader role can neither preview
         * nor download, so it must not grant search access either.</p>
         *
         * @param status the collaboration status
         * @param role the collaboration role
         * @return true if the collaboration should contribute a search role
         */
        static boolean isEffectiveCollaboration(final BoxCollaboration.Status status, final BoxCollaboration.Role role) {
            return status == BoxCollaboration.Status.ACCEPTED && role != null && role != BoxCollaboration.Role.UPLOADER;
        }

        /**
         * Generates a list of Fess search roles based on the file's collaborations.
         * This allows mapping Box permissions to Fess search permissions.
         * @return A list of role strings.
         */
        public List<String> getCollaborationRoles() {
            final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
            final List<String> roleList = new ArrayList<>();
            final List<com.box.sdk.BoxCollaboration.Info> collaborationList = getAllFileCollaborations();
            if (logger.isDebugEnabled()) {
                logger.debug("collaborationList: {}", collaborationList.size());
            }
            collaborationList.forEach(c -> {
                if (!isEffectiveCollaboration(c.getStatus(), c.getRole())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("skipping collaboration: status={}, role={}", c.getStatus(), c.getRole());
                    }
                    return;
                }
                final Info accessibleBy = c.getAccessibleBy();
                if (accessibleBy == null) {
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("accessibleBy: {}", accessibleBy.getJson());
                }
                switch (accessibleBy.getType()) {
                case USER: {
                    roleList.add(systemHelper.getSearchRoleByUser(accessibleBy.getID()));
                    if (StringUtil.isNotBlank(accessibleBy.getLogin())) {
                        roleList.add(systemHelper.getSearchRoleByUser(accessibleBy.getLogin()));
                    }
                    break;
                }
                case GROUP: {
                    roleList.add(systemHelper.getSearchRoleByGroup(accessibleBy.getID()));
                    if (StringUtil.isNotBlank(accessibleBy.getLogin())) {
                        roleList.add(systemHelper.getSearchRoleByGroup(accessibleBy.getLogin()));
                    }
                    break;
                }
                default:
                    if (logger.isDebugEnabled()) {
                        logger.debug("unknown accessibleBy type: {}", accessibleBy.getType());
                    }
                    break;
                }
            });
            return roleList;
        }
    }
}

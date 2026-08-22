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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.timer.TimeoutManager;
import org.codelibs.core.timer.TimeoutTask;
import org.codelibs.fess.crawler.client.AbstractCrawlerClient;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.exception.DataStoreException;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.IAccessTokenCache;
import com.box.sdk.InMemoryLRUAccessTokenCache;
import com.box.sdk.JWTEncryptionPreferences;

/**
 * A client for accessing Box resources.
 * It extends {@link AbstractCrawlerClient} and implements {@link AutoCloseable} for resource management.
 */
public class BoxClient extends AbstractCrawlerClient implements AutoCloseable {

    /**
     * Default constructor.
     */
    public BoxClient() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(BoxClient.class);

    /** Default interval in seconds for refreshing the access token. */
    protected static final String DEFAULT_REFRESH_TOKEN_INTERVAL = "3540";

    /** Parameter key for the base URL of the Box API. */
    protected static final String BASE_URL = "base_url";
    /** Parameter key for the client ID. */
    protected static final String CLIENT_ID_PARAM = "client_id";
    /** Parameter key for the client secret. */
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    /** Parameter key for the public key ID. */
    protected static final String PUBLIC_KEY_ID_PARAM = "public_key_id";
    /** Parameter key for the private key. */
    protected static final String PRIVATE_KEY_PARAM = "private_key";
    /** Parameter key for the passphrase for the private key. */
    protected static final String PASSPHRASE_PARAM = "passphrase";
    /** Parameter key for the enterprise ID. */
    protected static final String ENTERPRISE_ID_PARAM = "enterprise_id";
    /**
     * Parameter key for the maximum number of retries for API calls.
     *
     * <p>This is a different layer from {@link #MAX_RETRY_ATTEMPTS}: this one is a
     * plugin-level loop in {@link #getFiles} that retries only {@code 401} responses by
     * rebuilding the connection. It does not supersede {@link #MAX_RETRY_ATTEMPTS}, which
     * drives {@link BoxAPIConnection}'s own retry of {@code 429} and {@code >= 500}
     * responses, with jittered exponential backoff that honours {@code Retry-After}.</p>
     */
    protected static final String MAX_RETRY_COUNT = "max_retry_count";

    /** Parameter key for the proxy host. */
    protected static final String PROXY_HOST = "proxy_host";
    /** Parameter key for the proxy port. */
    protected static final String PROXY_PORT = "proxy_port";
    /** Parameter key for the proxy user name. */
    protected static final String PROXY_USERNAME = "proxy_username";
    /** Parameter key for the proxy password. */
    protected static final String PROXY_PASSWORD = "proxy_password";
    /** Parameter key for the refresh token interval. */
    protected static final String REFRESH_TOKEN_INTERVAL_PARAM = "refresh_token_interval";

    /** Parameter key for the connection timeout in milliseconds. */
    protected static final String CONNECT_TIMEOUT = "connect_timeout";
    /** Parameter key for the read timeout in milliseconds. */
    protected static final String READ_TIMEOUT = "read_timeout";
    /**
     * Parameter key for the SDK's retry count for 429 and 5xx responses.
     *
     * <p>This is a different layer from {@link #MAX_RETRY_COUNT}: this one drives
     * {@link BoxAPIConnection}'s own retry of {@code 429} and {@code >= 500} responses,
     * with jittered exponential backoff that honours {@code Retry-After}. It does not
     * supersede {@link #MAX_RETRY_COUNT}, which is a plugin-level loop in {@link #getFiles}
     * that retries only {@code 401} by rebuilding the connection.</p>
     */
    protected static final String MAX_RETRY_ATTEMPTS = "max_retry_attempts";

    /** Constant for the 'file' item type in Box. */
    protected static final String ITEM_TYPE_FILE = "file";
    /** Constant for the 'folder' item type in Box. */
    protected static final String ITEM_TYPE_FOLDER = "folder";

    /** The number of access tokens the shared token cache keeps in memory. */
    protected static final int TOKEN_CACHE_SIZE = 512;

    /** The base URL for the Box application. */
    protected String baseUrl;

    /** The active Box API connection. */
    protected BoxAPIConnection connection;

    /** The scheduled task for refreshing the access token. */
    protected TimeoutTask refreshTokenTask;

    /** The configuration for the Box API connection. */
    protected BoxConfig boxConfig;

    /** The maximum number of times to retry a failed API call. */
    protected int maxRetryCount;

    /**
     * Cache shared by all connections created from this client, including the ones
     * handed out by {@link #forUser(String)}, so that a per-user connection does not
     * perform its own token exchange.
     */
    protected IAccessTokenCache accessTokenCache;

    /**
     * The id of the user this client impersonates, or null for the enterprise service
     * account. Set once by {@link #forUser(String)} and re-applied by {@link #createConnection()}
     * whenever that method rebuilds the connection, so a mid-crawl reconnect (e.g. after a 401)
     * does not silently fall back to the service account identity.
     */
    protected String impersonatedUserId;

    @Override
    public synchronized void init() {
        if (baseUrl != null) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing BoxClient...");
        }
        this.baseUrl = getInitParameter(BASE_URL, "https://app.box.com");
        super.init();

        final String clientId = getInitParameter(CLIENT_ID_PARAM, StringUtil.EMPTY);
        final String clientSecret = getInitParameter(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
        final String publicKeyId = getInitParameter(PUBLIC_KEY_ID_PARAM, StringUtil.EMPTY);
        final String privateKey = getInitParameter(PRIVATE_KEY_PARAM, StringUtil.EMPTY).replace("\\n", "\n");
        final String passphrase = getInitParameter(PASSPHRASE_PARAM, StringUtil.EMPTY);
        final String enterpriseId = getInitParameter(ENTERPRISE_ID_PARAM, StringUtil.EMPTY);

        if (clientId.isEmpty() || clientSecret.isEmpty() || publicKeyId.isEmpty() || privateKey.isEmpty() || passphrase.isEmpty()
                || enterpriseId.isEmpty()) {
            throw new DataStoreException("Parameter '" + CLIENT_ID_PARAM + "', '" + CLIENT_SECRET_PARAM + "', '" + PUBLIC_KEY_ID_PARAM
                    + "', '" + PRIVATE_KEY_PARAM + "', '" + PASSPHRASE_PARAM + "', '" + ENTERPRISE_ID_PARAM + "' is required.");
        }

        final JWTEncryptionPreferences jwtPreferences = new JWTEncryptionPreferences();
        jwtPreferences.setPublicKeyID(publicKeyId);
        jwtPreferences.setPrivateKeyPassword(passphrase);
        jwtPreferences.setPrivateKey(privateKey);
        jwtPreferences.setEncryptionAlgorithm(EncryptionAlgorithm.RSA_SHA_256);
        boxConfig = new BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences);

        maxRetryCount = getInitParameter(MAX_RETRY_COUNT, 10, Integer.class);

        accessTokenCache = new InMemoryLRUAccessTokenCache(TOKEN_CACHE_SIZE);

        createConnection();
    }

    /**
     * Creates and configures a new Box API connection.
     * This method handles proxy settings and, for the primary (non-impersonating) client
     * only, schedules a task to refresh the access token periodically.
     */
    protected void createConnection() {
        if (logger.isDebugEnabled()) {
            logger.debug("creating Box Connection");
        }

        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }

        try {
            final BoxDeveloperEditionAPIConnection con =
                    BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig, accessTokenCache);
            configureConnection(con);
            if (impersonatedUserId != null) {
                // This client is one forUser() handed out. A rebuild (e.g. the 401 retry in
                // getFiles()) must keep impersonating the same user - otherwise every request
                // issued after the rebuild silently runs as the enterprise service account.
                con.asUser(impersonatedUserId);
            }
            connection = con;
            if (logger.isDebugEnabled()) {
                logger.debug("connected");
            }
        } catch (final BoxAPIException e) {
            throw new DataStoreException("Failed to create new connection. Box API Error : responseCode = " + e.getResponseCode()
                    + ", response = " + e.getResponse());
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create new connection.", e);
        }

        if (impersonatedUserId == null) {
            // Per-user clients are deliberately never closed - they share one token, so
            // revoking per user would break every other concurrently crawling user - and this
            // task is a `permanent` TimeoutManager registration that only close() cancels. If
            // every per-user client scheduled one, every 401-triggered reconnect would leak a
            // timer refreshing a dead crawl's connection forever. It is unnecessary there anyway:
            // BoxDeveloperEditionAPIConnection.canRefresh() always returns true and autoRefresh
            // defaults to true, so a per-user connection refreshes itself transparently the next
            // time it is used, and the As-User header survives that refresh because it lives in
            // a final field that authenticate()/refresh() never touch. This belt-and-braces timer
            // stays only for the single primary client that init() creates.
            refreshTokenTask = TimeoutManager.getInstance().addTimeoutTarget(() -> {
                if (connection != null) {
                    logger.info("Rrefreshing a current access token.");
                    try {
                        connection.refresh();
                    } catch (final Exception e) {
                        logger.warn("Failed to refresh an access token.", e);
                    }
                }
            }, Integer.parseInt(getInitParameter(REFRESH_TOKEN_INTERVAL_PARAM, DEFAULT_REFRESH_TOKEN_INTERVAL)), true);
        }
    }

    /**
     * Applies the connection settings shared by every connection this client creates:
     * proxy, connect/read timeouts and the SDK's own retry count.
     *
     * <p>Extracted from {@link #createConnection()} so that {@link #forUser(String)}
     * configures its per-user connection identically, instead of silently bypassing
     * the proxy or the timeouts.</p>
     *
     * @param con the connection to configure
     */
    protected void configureConnection(final BoxAPIConnection con) {
        final String proxyHost = getInitParameter(PROXY_HOST, StringUtil.EMPTY);
        final String proxyPort = getInitParameter(PROXY_PORT, StringUtil.EMPTY);
        if (StringUtil.isNotBlank(proxyHost) && StringUtil.isNotBlank(proxyPort)) {
            if (logger.isDebugEnabled()) {
                logger.debug("proxy: {}:{}", proxyHost, proxyPort);
            }
            con.setProxy((new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)))));

            final String proxyUsername = getInitParameter(PROXY_USERNAME, StringUtil.EMPTY);
            final String proxyPassword = getInitParameter(PROXY_PASSWORD, StringUtil.EMPTY);
            if (StringUtil.isNotBlank(proxyUsername) && StringUtil.isNotBlank(proxyPassword)) {
                con.setProxyBasicAuthentication(proxyUsername, proxyPassword);
            }
        }

        final int connectTimeout = getInitParameter(CONNECT_TIMEOUT, 0, Integer.class);
        if (connectTimeout > 0) {
            con.setConnectTimeout(connectTimeout);
        }
        final int readTimeout = getInitParameter(READ_TIMEOUT, 0, Integer.class);
        if (readTimeout > 0) {
            con.setReadTimeout(readTimeout);
        }
        con.setMaxRetryAttempts(getInitParameter(MAX_RETRY_ATTEMPTS, 5, Integer.class));
    }

    /**
     * Gets an initialization parameter as a String.
     * @param key the parameter key
     * @param defaultValue the default value if the parameter is not found
     * @return the parameter value
     */
    protected String getInitParameter(final String key, final String defaultValue) {
        return getInitParameter(key, defaultValue, String.class);
    }

    /**
     * Closes the Box client, revoking the current access token and stopping the refresh task.
     */
    @Override
    public void close() {
        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }
        if (connection != null) {
            connection.revokeToken();
        }
    }

    /**
     * Returns the base URL of the Box application.
     * @return the base URL
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Retrieves all enterprise users, optionally filtering by a term.
     * @param filterTerm the term to filter users by (can be null)
     * @param consumer a consumer to process each user info
     */
    public void getUsers(final String filterTerm, final Consumer<BoxUser.Info> consumer) {
        BoxUser.getAllEnterpriseUsers(connection, filterTerm).forEach(consumer);
    }

    /**
     * Gets the root folder for the current user.
     * @return the root folder
     */
    public BoxFolder getRootFolder() {
        return BoxFolder.getRootFolder(connection);
    }

    /**
     * Gets a specific folder by its ID.
     * @param folderId the ID of the folder
     * @return the folder object
     */
    public BoxFolder getFolder(final String folderId) {
        return new BoxFolder(connection, folderId);
    }

    /**
     * Gets the collaborations of a folder.
     *
     * @param folderId the ID of the folder
     * @return the folder's collaborations
     */
    public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
        return getFolder(folderId).getCollaborations();
    }

    /**
     * Recursively retrieves all files within a given folder and passes them to a consumer.
     * Folders are still traversed, but never surfaced - equivalent to calling
     * {@link #getFiles(BoxFolder, String[], Consumer, Consumer)} with a null folder consumer.
     *
     * @param folder the folder to start from
     * @param fields the fields to retrieve for each item
     * @param fileConsumer a consumer to process each file
     */
    public void getFiles(final BoxFolder folder, final String[] fields, final Consumer<BoxFile> fileConsumer) {
        getFiles(folder, fields, fileConsumer, null);
    }

    /**
     * Recursively retrieves all files - and, if {@code folderConsumer} is non-null, all
     * descendant folders - within a given folder and passes them to the corresponding consumer.
     *
     * <p>A folder is always traversed regardless of {@code folderConsumer}, since files inside
     * it must still be reached. Passing {@code null} only stops the folder itself from being
     * surfaced to the caller; it costs nothing beyond the one null check per folder, since no
     * extra Box API call is made for it.</p>
     *
     * @param folder the folder to start from
     * @param fields the fields to retrieve for each item
     * @param fileConsumer a consumer to process each file
     * @param folderConsumer a consumer to process each descendant folder, or {@code null} to
     *        only recurse into folders without surfacing them
     */
    public void getFiles(final BoxFolder folder, final String[] fields, final Consumer<BoxFile> fileConsumer,
            final Consumer<BoxFolder> folderConsumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Crawling folder {}", folder.getID());
        }
        final Iterable<BoxItem.Info> children;
        if (fields != null) {
            children = folder.getChildren(fields);
        } else {
            children = folder.getChildren();
        }
        final Consumer<BoxItem.Info> processor = info -> {
            if (logger.isDebugEnabled()) {
                logger.debug("item info: {}:{}", info.getID(), info.getName());
            }
            switch (info.getType()) {
            case ITEM_TYPE_FILE:
                fileConsumer.accept(new BoxFile(connection, info.getID()));
                break;
            case ITEM_TYPE_FOLDER:
                final BoxFolder childFolder = getFolder(info.getID());
                if (folderConsumer != null) {
                    folderConsumer.accept(childFolder);
                }
                getFiles(childFolder, fields, fileConsumer, folderConsumer);
                break;
            default:
                logger.warn("Unknown item type: {}", info.getType());
                break;
            }
        };
        children.forEach(info -> {
            for (int i = 0; i < maxRetryCount; i++) {
                try {
                    processor.accept(info);
                    return;
                } catch (final BoxAPIResponseException e) {
                    if (e.getResponseCode() != 401) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to access {}", info.getID(), e);
                    }
                } catch (final RuntimeException e) {
                    final Set<Throwable> exceptionSet = new HashSet<>();
                    Throwable cause = e.getCause();
                    while (cause != null && !(cause instanceof BoxAPIResponseException)) {
                        exceptionSet.add(cause);
                        cause = cause.getCause();
                        if (cause != null && exceptionSet.contains(cause)) {
                            cause = null;
                        }
                    }
                    if ((cause == null) || (((BoxAPIException) cause).getResponseCode() != 401)) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to access {}", info.getID(), e);
                    }
                }
                final BoxAPIConnection con = connection;
                synchronized (boxConfig) {
                    if (con == connection) {
                        createConnection();
                    }
                }
            }
        });
    }

    /**
     * Gets an input stream for the content of a given file.
     * @param file the file to download
     * @return an input stream for the file content
     * @throws CrawlingAccessException if the download fails
     */
    public InputStream getFileInputStream(final BoxFile file) {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream((int) maxCachedContentSize, "crawler-BoxClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            file.download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            }
            return new TemporaryFileInputStream(dfos.getFile());
        } catch (final BoxAPIException e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getID() + " -> " + e.getResponse(), e);
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getID(), e);
        }
    }

    /**
     * Switches the API connection to act as the application's service account.
     */
    public void asSelf() {
        connection.asSelf();
    }

    /**
     * Switches the API connection to act as a specific user.
     * @param userId the ID of the user to impersonate
     */
    public void asUser(final String userId) {
        connection.asUser(userId);
    }

    /**
     * Returns a client bound to a single user, backed by its own connection.
     *
     * <p>{@link #asUser(String)} mutates the connection it is called on. Worker
     * threads crawling one user's files can still be running when the caller moves
     * on to the next user, so mutating a shared connection risks issuing a request
     * under the wrong identity. Each user therefore gets an independent connection
     * that is never touched by any other user's crawl; the returned client shares
     * this client's access token cache so that impersonating the user does not
     * require its own token exchange.</p>
     *
     * @param userId the ID of the user to act as
     * @return a client scoped to that user
     */
    public BoxClient forUser(final String userId) {
        final BoxClient userClient = new BoxClient();
        userClient.baseUrl = baseUrl;
        userClient.boxConfig = boxConfig;
        userClient.maxRetryCount = maxRetryCount;
        userClient.accessTokenCache = accessTokenCache;
        userClient.impersonatedUserId = userId;
        // init() is never called on userClient - it would no-op anyway, since baseUrl is
        // already set above - so maxCachedContentSize (the only inherited field this class
        // reads) must be copied explicitly or file downloads would silently stop honouring it.
        userClient.setMaxCachedContentSize(maxCachedContentSize);
        userClient.setInitParameterMap(initParamMap);
        final BoxDeveloperEditionAPIConnection con =
                BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig, accessTokenCache);
        configureConnection(con);
        con.asUser(userId);
        userClient.connection = con;
        return userClient;
    }

}

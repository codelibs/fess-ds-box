/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.timer.TimeoutManager;
import org.codelibs.core.timer.TimeoutTask;
import org.codelibs.fess.crawler.client.AbstractCrawlerClient;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.JWTEncryptionPreferences;

public class BoxClient extends AbstractCrawlerClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BoxClient.class);

    protected static final String DEFAULT_REFRESH_TOKEN_INTERVAL = "3540";

    protected static final String BASE_URL = "base_url";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String PUBLIC_KEY_ID_PARAM = "public_key_id";
    protected static final String PRIVATE_KEY_PARAM = "private_key";
    protected static final String PASSPHRASE_PARAM = "passphrase";
    protected static final String ENTERPRISE_ID_PARAM = "enterprise_id";
    protected static final String MAX_RETRY_COUNT = "max_retry_count";

    protected static final String PROXY_HOST = "proxy_host";
    protected static final String PROXY_PORT = "proxy_port";
    protected static final String REFRESH_TOKEN_INTERVAL_PARAM = "refresh_token_interval";

    protected static final String ITEM_TYPE_FILE = "file";
    protected static final String ITEM_TYPE_FOLDER = "folder";

    protected String baseUrl;

    protected BoxAPIConnection connection;

    protected TimeoutTask refreshTokenTask;

    protected BoxConfig boxConfig;

    protected int maxRetryCount;

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

        createConnction();
    }

    protected void createConnction() {
        if (logger.isDebugEnabled()) {
            logger.debug("creating Box Connnection");
        }

        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }

        try {
            final BoxDeveloperEditionAPIConnection con = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
            final String proxyHost = getInitParameter(PROXY_HOST, StringUtil.EMPTY);
            final String proxyPort = getInitParameter(PROXY_PORT, StringUtil.EMPTY);
            if (StringUtil.isNotBlank(proxyHost) && StringUtil.isNotBlank(proxyPort)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("proxy: {}:{}", proxyHost, proxyPort);
                }
                con.setProxy((new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)))));
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

    protected String getInitParameter(final String key, final String defaultValue) {
        return getInitParameter(key, defaultValue, String.class);
    }

    @Override
    public void close() {
        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }
        if (connection != null) {
            connection.revokeToken();
        }
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void getUsers(final String filterTerm, final Consumer<BoxUser.Info> consumer) {
        BoxUser.getAllEnterpriseUsers(connection, filterTerm).forEach(consumer);
    }

    public BoxFolder getRootFolder() {
        return BoxFolder.getRootFolder(connection);
    }

    public BoxFolder getFolder(final String folderId) {
        return new BoxFolder(connection, folderId);
    }

    public void getFiles(final BoxFolder folder, final String userId, final String[] fields, final Consumer<BoxFile> consumer) {
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
                consumer.accept(new BoxFile(connection, info.getID()));
                break;
            case ITEM_TYPE_FOLDER:
                getFiles(getFolder(info.getID()), userId, fields, consumer);
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
                        createConnction();
                    }
                }
            }
        });
    }

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

    public void asSelf() {
        connection.asSelf();
    }

    public void asUser(final String userId) {
        connection.asUser(userId);
    }

}

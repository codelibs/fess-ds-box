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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.JWTEncryptionPreferences;

public class BoxClientConnection implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BoxClientConnection.class);

    protected static final String BASE_URL = "base_url";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String PUBLIC_KEY_ID_PARAM = "public_key_id";
    protected static final String PRIVATE_KEY_PARAM = "private_key";
    protected static final String PASSPHRASE_PARAM = "passphrase";
    protected static final String ENTERPRISE_ID_PARAM = "enterprise_id";

    protected static final String PROXY_HOST = "proxy_host";
    protected static final String PROXY_PORT = "proxy_port";
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected static final String ITEM_TYPE_FILE = "file";
    protected static final String ITEM_TYPE_FOLDER = "folder";

    protected final Map<String, String> params;
    protected final BoxAPIConnection connection;

    protected int maxCachedContentSize = 1024 * 1024;

    protected final String baseUrl;

    public BoxClientConnection(final Map<String, String> params) {
        this.params = params;
        this.baseUrl = params.getOrDefault(BASE_URL, "https://app.box.com");
        this.connection = newConnection();
        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
    }

    protected BoxAPIConnection newConnection() {
        final String clientId = params.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY);
        final String clientSecret = params.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
        final String publicKeyId = params.getOrDefault(PUBLIC_KEY_ID_PARAM, StringUtil.EMPTY);
        final String privateKey = params.getOrDefault(PRIVATE_KEY_PARAM, StringUtil.EMPTY).replaceAll("\\\\n", "\n");
        final String passphrase = params.getOrDefault(PASSPHRASE_PARAM, StringUtil.EMPTY);
        final String enterpriseId = params.getOrDefault(ENTERPRISE_ID_PARAM, StringUtil.EMPTY);

        if (clientId.isEmpty() || clientSecret.isEmpty() || publicKeyId.isEmpty() || privateKey.isEmpty() || passphrase.isEmpty()
                || enterpriseId.isEmpty()) {
            throw new DataStoreException(
                    "Parameter '" + CLIENT_ID_PARAM + "', '" + CLIENT_SECRET_PARAM + "', '" + PUBLIC_KEY_ID_PARAM + "', '"
                            + PRIVATE_KEY_PARAM + "', '" + PASSPHRASE_PARAM + "', '" + ENTERPRISE_ID_PARAM + "' is required.");
        }

        final JWTEncryptionPreferences jwtPreferences = new JWTEncryptionPreferences();
        jwtPreferences.setPublicKeyID(publicKeyId);
        jwtPreferences.setPrivateKeyPassword(passphrase);
        jwtPreferences.setPrivateKey(privateKey);
        jwtPreferences.setEncryptionAlgorithm(EncryptionAlgorithm.RSA_SHA_256);
        final BoxConfig boxConfig = new BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences);
        final BoxDeveloperEditionAPIConnection connection = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);

        final String proxyHost = params.get(PROXY_HOST);
        final String proxyPort = params.get(PROXY_PORT);
        if (StringUtil.isNotBlank(proxyHost) && StringUtil.isNotBlank(proxyPort)) {
            connection.setProxy((new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)))));
        }
        return connection;
    }

    @Override
    public void close() {
        connection.revokeToken();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void getUsers(final Consumer<BoxUser> consumer) {
        getUsers(null, consumer);
    }

    public void getUsers(final String filterTerm, final Consumer<BoxUser> consumer) {
        connection.asSelf();
        BoxUser.getAllEnterpriseUsers(connection, filterTerm).forEach(info -> consumer.accept(info.getResource()));
    }

    public BoxFolder getRootFolder() {
        return getRootFolder(null);
    }

    public BoxFolder getRootFolder(final String userId) {
        if (StringUtil.isNotBlank(userId)) {
            connection.asUser(userId);
        } else {
            connection.asSelf();
        }
        return BoxFolder.getRootFolder(connection);
    }

    public BoxFolder getFolder(final String folderId) {
        return getFolder(folderId, null);
    }

    public BoxFolder getFolder(final String folderId, final String userId) {
        if (StringUtil.isNotBlank(userId)) {
            connection.asUser(userId);
        } else {
            connection.asSelf();
        }
        return new BoxFolder(connection, folderId);
    }

    public void getFiles(final BoxFolder folder, final String[] fields, final Consumer<BoxFile> consumer) {
        getFiles(folder, null, fields, consumer);
    }

    public void getFiles(final BoxFolder folder, final String userId, final String[] fields, final Consumer<BoxFile> consumer) {
        if (StringUtil.isNotBlank(userId)) {
            connection.asUser(userId);
        } else {
            connection.asSelf();
        }
        // TODO use getChildrenRange()
        final Iterable<BoxItem.Info> children;
        if (fields != null) {
            children = folder.getChildren(fields);
        } else {
            children = folder.getChildren();
        }
        children.forEach(info -> {
            switch (info.getType()) {
            case ITEM_TYPE_FILE:
                consumer.accept(new BoxFile(connection, info.getID()));
                break;
            case ITEM_TYPE_FOLDER:
                getFiles(getFolder(info.getID()), userId, fields, consumer);
                break;
            }
        });
    }

    public void getFiles(final BoxFolder folder, final Consumer<BoxFile> consumer) {
        getFiles(folder, null, null, consumer);
    }

    public void getFiles(final BoxFolder folder, final String userId, final Consumer<BoxFile> consumer) {
        getFiles(folder, userId, null, consumer);
    }

    public InputStream getFileInputStream(final BoxFile file) {
        try (final DeferredFileOutputStream dfos = new DeferredFileOutputStream(maxCachedContentSize, "crawler-BoxClientConnection-", ".out",
                SystemUtils.getJavaIoTmpDir())) {
            file.download(dfos);
            dfos.flush();
            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            } else {
                return new TemporaryFileInputStream(dfos.getFile());
            }
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + file.getID(), e);
        }
    }

}
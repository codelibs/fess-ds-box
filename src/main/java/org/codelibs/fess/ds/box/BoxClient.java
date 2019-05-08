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

import com.box.sdk.*;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;

public class BoxClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(BoxClient.class);

    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String PUBLIC_KEY_ID_PARAM = "public_key_id";
    protected static final String PRIVATE_KEY_PARAM = "private_key";
    protected static final String PASSPHRASE_PARAM = "passphrase";
    protected static final String ENTERPRISE_ID_PARAM = "enterprise_id";

    protected static final String PROXY_HOST = "proxy_host";
    protected static final String PROXY_PORT = "proxy_port";
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected BoxAPIConnection connection;
    protected final Map<String, String> params;

    protected int maxCachedContentSize = 1024 * 1024;

    public BoxClient(final Map<String, String> params) {
        this.params = params;
        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
    }

    protected BoxAPIConnection getConnection() {
        if (connection == null) {
            connection = newConnection();
        }
        return connection;
    }

    protected BoxDeveloperEditionAPIConnection newConnection() {
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
        if (connection != null) {
            connection.revokeToken();
        }
    }

}

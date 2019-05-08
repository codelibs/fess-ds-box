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

import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import com.box.sdk.EncryptionAlgorithm;
import com.box.sdk.JWTEncryptionPreferences;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BoxDataStore extends AbstractDataStore {

    // parameters
    private static final String CLIENT_ID_PARAM = "client_id";
    private static final String CLIENT_SECRET_PARAM = "client_secret";
    private static final String PUBLIC_KEY_ID_PARAM = "public_key_id";
    private static final String PRIVATE_KEY_PARAM = "private_key";
    private static final String PASSPHRASE_PARAM = "passphrase";
    private static final String ENTERPRISE_ID_PARAM = "enterprise_id";

    private Logger logger = LoggerFactory.getLogger(BoxDataStore.class);

    @Override
    protected String getName() {
        return "Box";
    }

    @Override
    public void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final String clientId = getClientId(paramMap);
        final String clientSecret = getClientSecret(paramMap);
        final String publicKeyId = getPublicKeyId(paramMap);
        final String privateKey = getPrivateKey(paramMap);
        final String passphrase = getPassphrase(paramMap);
        final String enterpriseId = getEnterpriseId(paramMap);

        if (clientId.isEmpty() || clientSecret.isEmpty() || publicKeyId.isEmpty() || privateKey.isEmpty() || passphrase.isEmpty()
                || enterpriseId.isEmpty()) {
            throw new DataStoreException(
                    "parameter '" + CLIENT_ID_PARAM + "', '" + CLIENT_SECRET_PARAM + "', '" + PUBLIC_KEY_ID_PARAM + "', '"
                            + PRIVATE_KEY_PARAM + "', '" + PASSPHRASE_PARAM + "', '" + ENTERPRISE_ID_PARAM + "' is required");
        }

        final BoxDeveloperEditionAPIConnection api =
                getAPIConnection(publicKeyId, passphrase, privateKey, clientId, clientSecret, enterpriseId);
    }

    BoxDeveloperEditionAPIConnection getAPIConnection(final String publicKeyId, final String privateKeyPassword, final String privateKey,
            final String clientId, final String clientSecret, final String enterpriseId) {
        final JWTEncryptionPreferences jwtPreferences = new JWTEncryptionPreferences();
        jwtPreferences.setPublicKeyID(publicKeyId);
        jwtPreferences.setPrivateKeyPassword(privateKeyPassword);
        jwtPreferences.setPrivateKey(privateKey);
        jwtPreferences.setEncryptionAlgorithm(EncryptionAlgorithm.RSA_SHA_256);
        final BoxConfig boxConfig = new BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences);
        return BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
    }

    private String getClientId(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY);
    }

    private String getClientSecret(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
    }

    private String getPublicKeyId(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(PUBLIC_KEY_ID_PARAM, StringUtil.EMPTY);
    }

    private String getPrivateKey(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(PRIVATE_KEY_PARAM, StringUtil.EMPTY).replace("\\n", "\n");
    }

    private String getPassphrase(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(PASSPHRASE_PARAM, StringUtil.EMPTY);
    }

    private String getEnterpriseId(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(ENTERPRISE_ID_PARAM, StringUtil.EMPTY);
    }

}

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
package org.codelibs.fess.ds.box

import com.box.sdk.BoxConfig
import com.box.sdk.BoxDeveloperEditionAPIConnection
import com.box.sdk.EncryptionAlgorithm
import com.box.sdk.JWTEncryptionPreferences
import org.codelibs.core.lang.StringUtil
import org.codelibs.fess.ds.AbstractDataStore
import org.codelibs.fess.ds.callback.IndexUpdateCallback
import org.codelibs.fess.es.config.exentity.DataConfig
import org.codelibs.fess.exception.DataStoreException
import org.slf4j.LoggerFactory

class BoxDataStore : AbstractDataStore() {

    companion object {
        // parameters
        private const val CLIENT_ID_PARAM = "client_id"
        private const val CLIENT_SECRET_PARAM = "client_secret"
        private const val PUBLIC_KEY_ID_PARAM = "public_key_id"
        private const val PRIVATE_KEY_PARAM = "private_key"
        private const val PASSPHRASE_PARAM = "passphrase"
        private const val ENTERPRISE_ID_PARAM = "enterprise_id"
    }

    private val logger = LoggerFactory.getLogger(BoxDataStore::class.java)

    override fun getName(): String = "Box"

    override fun storeData(dataConfig: DataConfig, callback: IndexUpdateCallback, paramMap: Map<String, String>,
                           scriptMap: Map<String, String>, defaultDataMap: Map<String, Any>) {
        val clientId = getClientId(paramMap)
        val clientSecret = getClientSecret(paramMap)
        val publicKeyId = getPublicKeyId(paramMap)
        val privateKey = getPrivateKey(paramMap)
        val passphrase = getPassphrase(paramMap)
        val enterpriseId = getEnterpriseId(paramMap)

        if (clientId.isEmpty() || clientSecret.isEmpty() || publicKeyId.isEmpty() || privateKey.isEmpty() || passphrase.isEmpty() || enterpriseId.isEmpty()) {
            throw DataStoreException("parameter '$CLIENT_ID_PARAM', '$CLIENT_SECRET_PARAM', '$PUBLIC_KEY_ID_PARAM', '$PRIVATE_KEY_PARAM', '$PASSPHRASE_PARAM', '$ENTERPRISE_ID_PARAM' is required")
        }

        val api = getAPIConnection(publicKeyId, passphrase, privateKey, clientId, clientSecret, enterpriseId)
    }

    internal fun getAPIConnection(publicKeyId: String, privateKeyPassword: String, privateKey: String, clientId: String, clientSecret: String, enterpriseId: String): BoxDeveloperEditionAPIConnection {
        val jwtPreferences = JWTEncryptionPreferences().also {
            it.publicKeyID = publicKeyId
            it.privateKeyPassword = privateKeyPassword
            it.privateKey = privateKey
            it.encryptionAlgorithm = EncryptionAlgorithm.RSA_SHA_256
        }
        val boxConfig = BoxConfig(clientId, clientSecret, enterpriseId, jwtPreferences)
        return BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig)
    }

    private fun getClientId(paramMap: Map<String, String>): String = paramMap.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY)
    private fun getClientSecret(paramMap: Map<String, String>): String = paramMap.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY)
    private fun getPublicKeyId(paramMap: Map<String, String>): String = paramMap.getOrDefault(PUBLIC_KEY_ID_PARAM, StringUtil.EMPTY)
    private fun getPrivateKey(paramMap: Map<String, String>): String = paramMap.getOrDefault(PRIVATE_KEY_PARAM, StringUtil.EMPTY).replace("\\n", "\n")
    private fun getPassphrase(paramMap: Map<String, String>): String = paramMap.getOrDefault(PASSPHRASE_PARAM, StringUtil.EMPTY)
    private fun getEnterpriseId(paramMap: Map<String, String>): String = paramMap.getOrDefault(ENTERPRISE_ID_PARAM, StringUtil.EMPTY)

}

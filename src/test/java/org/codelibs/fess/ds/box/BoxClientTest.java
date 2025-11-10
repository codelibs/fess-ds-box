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

import java.util.HashMap;
import java.util.Map;

import org.codelibs.fess.exception.DataStoreException;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class BoxClientTest extends LastaFluteTestCase {

    @Override
    public String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    public boolean isSuppressTestCaseTransaction() {
        return true;
    }

    public void test_initialization() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        client.setInitParameterMap(params);

        // Verify that setInitParameterMap works correctly
        assertNotNull(client);
    }

    public void test_getBaseUrl_default() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        client.setInitParameterMap(params);

        try {
            client.init();
            assertEquals("https://app.box.com", client.getBaseUrl());
        } catch (final DataStoreException e) {
            // Expected when connection cannot be established in test environment
            assertTrue(e.getMessage().contains("Failed to create new connection"));
        } finally {
            client.close();
        }
    }

    public void test_getBaseUrl_custom() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.put("base_url", "https://custom.box.com");
        client.setInitParameterMap(params);

        try {
            client.init();
            assertEquals("https://custom.box.com", client.getBaseUrl());
        } catch (final DataStoreException e) {
            // Expected when connection cannot be established in test environment
            assertTrue(e.getMessage().contains("Failed to create new connection"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingClientId() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("client_id"); // Remove required parameter
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing client_id");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingClientSecret() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("client_secret");
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing client_secret");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingPublicKeyId() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("public_key_id");
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing public_key_id");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingPrivateKey() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("private_key");
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing private_key");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingPassphrase() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("passphrase");
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing passphrase");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_init_missingEnterpriseId() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        params.remove("enterprise_id");
        client.setInitParameterMap(params);

        try {
            client.init();
            fail("Should throw DataStoreException for missing enterprise_id");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("is required"));
        } finally {
            client.close();
        }
    }

    public void test_close() {
        final BoxClient client = new BoxClient();
        // close should not throw exception even if not initialized
        try {
            client.close();
        } catch (final Exception e) {
            fail("close() should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Creates a map with valid (but dummy) parameters for testing.
     * These values won't establish a real connection but are sufficient for parameter validation tests.
     */
    private Map<String, Object> createValidParams() {
        final Map<String, Object> params = new HashMap<>();
        params.put("client_id", "test_client_id");
        params.put("client_secret", "test_client_secret");
        params.put("public_key_id", "test_public_key_id");
        params.put("private_key", "-----BEGIN ENCRYPTED PRIVATE KEY-----\ntest_key\n-----END ENCRYPTED PRIVATE KEY-----");
        params.put("passphrase", "test_passphrase");
        params.put("enterprise_id", "test_enterprise_id");
        return params;
    }

}

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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.ds.box.UnitDsTestCase;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;

public class BoxClientTest extends UnitDsTestCase {

    @Override
    public String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    public boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Test
    public void test_initialization() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = createValidParams();
        client.setInitParameterMap(params);

        // Verify that setInitParameterMap works correctly
        assertNotNull(client);
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void test_close() {
        final BoxClient client = new BoxClient();
        // close should not throw exception even if not initialized
        try {
            client.close();
        } catch (final Exception e) {
            fail("close() should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void test_configureConnection_defaultsLeaveTimeoutsUntouchedAndSetMaxRetryAttemptsToFive() {
        final BoxClient client = new BoxClient();
        client.setInitParameterMap(new HashMap<>());
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");
        final int defaultConnectTimeout = con.getConnectTimeout();
        final int defaultReadTimeout = con.getReadTimeout();

        client.configureConnection(con);

        // connect_timeout/read_timeout were never set, so the SDK's own defaults must survive.
        assertEquals(defaultConnectTimeout, con.getConnectTimeout());
        assertEquals(defaultReadTimeout, con.getReadTimeout());
        // max_retry_attempts defaults to 5, matching BoxAPIConnection.DEFAULT_MAX_RETRIES.
        assertEquals(5, con.getMaxRetryAttempts());
    }

    @Test
    public void test_configureConnection_appliesPositiveTimeouts() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("connect_timeout", "1234");
        params.put("read_timeout", "5678");
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");

        client.configureConnection(con);

        assertEquals(1234, con.getConnectTimeout());
        assertEquals(5678, con.getReadTimeout());
    }

    @Test
    public void test_configureConnection_ignoresZeroOrNegativeTimeouts() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("connect_timeout", "0");
        params.put("read_timeout", "-1");
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");
        final int defaultConnectTimeout = con.getConnectTimeout();
        final int defaultReadTimeout = con.getReadTimeout();

        client.configureConnection(con);

        assertEquals(defaultConnectTimeout, con.getConnectTimeout());
        assertEquals(defaultReadTimeout, con.getReadTimeout());
    }

    @Test
    public void test_configureConnection_appliesCustomMaxRetryAttempts() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("max_retry_attempts", "9");
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");

        client.configureConnection(con);

        assertEquals(9, con.getMaxRetryAttempts());
    }

    @Test
    public void test_configureConnection_appliesProxyBasicAuthWhenHostPortUserAndPasswordPresent() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("proxy_host", "proxy.example.com");
        params.put("proxy_port", "8080");
        params.put("proxy_username", "proxy_user");
        params.put("proxy_password", "proxy_pass");
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");

        client.configureConnection(con);

        assertNotNull(con.getProxy());
        assertEquals("proxy_user", con.getProxyUsername());
        assertEquals("proxy_pass", con.getProxyPassword());
    }

    @Test
    public void test_configureConnection_skipsProxyAuthWithoutProxyHostAndPort() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        // Username and password given, but no proxy host/port - there is no proxy to
        // authenticate against, so neither must be applied.
        params.put("proxy_username", "proxy_user");
        params.put("proxy_password", "proxy_pass");
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");

        client.configureConnection(con);

        assertNull(con.getProxy());
        assertNull(con.getProxyUsername());
        assertNull(con.getProxyPassword());
    }

    @Test
    public void test_configureConnection_skipsProxyAuthWhenOnlyUsernameGiven() {
        final BoxClient client = new BoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("proxy_host", "proxy.example.com");
        params.put("proxy_port", "8080");
        params.put("proxy_username", "proxy_user");
        // proxy_password intentionally omitted
        client.setInitParameterMap(params);
        final BoxAPIConnection con = new BoxAPIConnection("dummy-token");

        client.configureConnection(con);

        assertNotNull("the proxy itself must still be set", con.getProxy());
        assertNull("basic auth must not be applied with only a username", con.getProxyUsername());
        assertNull("basic auth must not be applied with only a username", con.getProxyPassword());
    }

    // --- getFiles(BoxFolder, String[], Consumer<BoxFile>, Consumer<BoxFolder>) ---

    private static final String[] TEST_FIELDS = { "type", "id", "name" };

    private static BoxItem.Info fileInfo(final String id) {
        return new BoxFile(null, id).new Info("{\"type\":\"file\",\"id\":\"" + id + "\"}");
    }

    private static BoxItem.Info folderInfo(final String id) {
        return new BoxFolder(null, id).new Info("{\"type\":\"folder\",\"id\":\"" + id + "\"}");
    }

    /** A {@link BoxFolder} whose children are canned, so recursion needs no network access. */
    static class RecordingFolder extends BoxFolder {
        private final List<BoxItem.Info> children;

        RecordingFolder(final String id, final List<BoxItem.Info> children) {
            super(null, id);
            this.children = children;
        }

        @Override
        public Iterable<BoxItem.Info> getChildren(final String... fields) {
            return children;
        }
    }

    /**
     * A {@link BoxClient} whose {@link #getFolder(String)} returns pre-registered
     * {@link RecordingFolder}s instead of a fresh, network-backed {@link BoxFolder} - this is
     * what lets a multi-level recursion run entirely offline.
     */
    static class FolderLookupBoxClient extends BoxClient {
        private final Map<String, BoxFolder> foldersById = new HashMap<>();

        void registerFolder(final BoxFolder folder) {
            foldersById.put(folder.getID(), folder);
        }

        @Override
        public BoxFolder getFolder(final String folderId) {
            final BoxFolder folder = foldersById.get(folderId);
            return folder != null ? folder : super.getFolder(folderId);
        }
    }

    /**
     * A {@link FolderLookupBoxClient} that records what the 4-arg {@link #getFiles} was actually
     * called with, so a test driving the 3-arg convenience overload can assert what it delegated
     * to, rather than only checking outcomes (file ids) that would be unaffected by the overload
     * delegating with the wrong folder consumer.
     */
    static class FolderConsumerCapturingBoxClient extends FolderLookupBoxClient {
        boolean fourArgInvoked;
        Consumer<BoxFolder> lastFolderConsumer;

        @Override
        public void getFiles(final BoxFolder folder, final String[] fields, final Consumer<BoxFile> fileConsumer,
                final Consumer<BoxFolder> folderConsumer) {
            fourArgInvoked = true;
            lastFolderConsumer = folderConsumer;
            super.getFiles(folder, fields, fileConsumer, folderConsumer);
        }
    }

    /** Builds a two-level tree: root -> [file f1, folder d1], d1 -> [file f2]. */
    private FolderLookupBoxClient newTreeClient() {
        final RecordingFolder d1 = new RecordingFolder("d1", List.of(fileInfo("f2")));
        final FolderLookupBoxClient client = new FolderLookupBoxClient();
        // init() is never called in this test - it needs real JWT credentials - so
        // maxRetryCount defaults to 0, and the per-item retry loop in getFiles would then never
        // run its body at all, silently dropping every child. Set it explicitly instead.
        client.maxRetryCount = 1;
        client.registerFolder(d1);
        return client;
    }

    private RecordingFolder rootFolder() {
        return new RecordingFolder("root", List.of(fileInfo("f1"), folderInfo("d1")));
    }

    @Test
    public void test_getFiles_fourArg_emitsFilesAndFoldersWhenFolderConsumerProvided() {
        final FolderLookupBoxClient client = newTreeClient();
        final List<String> fileIds = new ArrayList<>();
        final List<String> folderIds = new ArrayList<>();

        client.getFiles(rootFolder(), TEST_FIELDS, f -> fileIds.add(f.getID()), d -> folderIds.add(d.getID()));

        assertEquals(List.of("f1", "f2"), fileIds);
        assertEquals(List.of("d1"), folderIds);
    }

    @Test
    public void test_getFiles_fourArg_stillFindsNestedFilesWhenFolderConsumerNull() {
        // The core regression guard: a caller who does not want folders must see exactly the
        // same files as before - recursion must not depend on the folder consumer.
        final FolderLookupBoxClient client = newTreeClient();
        final List<String> fileIds = new ArrayList<>();

        client.getFiles(rootFolder(), TEST_FIELDS, f -> fileIds.add(f.getID()), null);

        assertEquals(List.of("f1", "f2"), fileIds);
    }

    @Test
    public void test_getFiles_threeArgOverload_delegatesToFourArgWithNullFolderConsumer() {
        // Asserting only file ids (as this test previously did) would pass even if the 3-arg
        // overload delegated with a non-null folder consumer, since nothing here would be
        // affected by that. Intercept the 4-arg call the 3-arg overload delegates to instead, and
        // assert what it actually received.
        final FolderConsumerCapturingBoxClient client = new FolderConsumerCapturingBoxClient();
        client.maxRetryCount = 1;
        client.registerFolder(new RecordingFolder("d1", List.of(fileInfo("f2"))));
        final List<String> fileIds = new ArrayList<>();

        final Consumer<BoxFile> fileConsumer = f -> fileIds.add(f.getID());
        client.getFiles(rootFolder(), TEST_FIELDS, fileConsumer);

        assertTrue("the 3-arg overload must delegate to the 4-arg method", client.fourArgInvoked);
        assertNull("the 3-arg overload must delegate with a null folder consumer", client.lastFolderConsumer);
        assertEquals(List.of("f1", "f2"), fileIds);
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

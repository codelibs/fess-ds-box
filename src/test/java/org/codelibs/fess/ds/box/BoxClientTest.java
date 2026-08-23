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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.crawler.client.AbstractCrawlerClient;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.ds.box.UnitDsTestCase;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.InMemoryLRUAccessTokenCache;
import com.box.sdk.JWTEncryptionPreferences;

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
    public void test_close_failingRevokeDoesNotPropagate() {
        // storeData wraps the client in try-with-resources, so a revoke that throws at the very
        // end of an otherwise successful crawl would fail the whole job after every document had
        // already been indexed.
        final RevokeRecordingConnection connection = new RevokeRecordingConnection(true);
        final BoxClient client = new BoxClient();
        client.connection = connection;

        try {
            client.close();
        } catch (final Exception e) {
            fail("a failing revokeToken() must not propagate out of close(): " + e);
        }

        assertTrue("close() must still have attempted the revoke", connection.revoked);
    }

    @Test
    public void test_close_revokesTheToken() {
        // The complement of the test above: swallowing the failure must not have turned into
        // skipping the call.
        final RevokeRecordingConnection connection = new RevokeRecordingConnection(false);
        final BoxClient client = new BoxClient();
        client.connection = connection;

        client.close();

        assertTrue("close() must revoke the access token", connection.revoked);
    }

    /** A connection that records - and optionally fails - the {@code revokeToken()} call. */
    static class RevokeRecordingConnection extends BoxAPIConnection {
        private final boolean failing;
        boolean revoked;

        RevokeRecordingConnection(final boolean failing) {
            super("dummy-token");
            this.failing = failing;
        }

        @Override
        public void revokeToken() {
            revoked = true;
            if (failing) {
                throw new BoxAPIException("simulated revoke failure", 401, "unauthorized");
            }
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

    // --- getFiles: the 401 retry loop ---

    /**
     * A {@link FolderLookupBoxClient} that records - instead of performing - the two side effects
     * of a retry: rebuilding the connection (a JWT token exchange) and waiting between attempts.
     */
    static class RetryRecordingBoxClient extends FolderLookupBoxClient {
        int reconnects;
        int sleeps;

        @Override
        protected void createConnection() {
            reconnects++;
        }

        @Override
        protected void sleepForRetry() {
            sleeps++;
        }
    }

    @Test
    public void test_getFiles_persistent401_sleepsBetweenAttemptsAndGivesUpWithAWarning() {
        // Every attempt rebuilds the connection, so without a pause a persistently-401 item
        // would burn max_retry_count JWT token exchanges back to back against an API limited to
        // 1000 requests per minute per user. And when the loop is exhausted the item simply
        // vanishes, so at minimum it must say so.
        final RetryRecordingBoxClient client = new RetryRecordingBoxClient();
        client.maxRetryCount = 3;
        final AtomicInteger attempts = new AtomicInteger();
        final List<String> warnings = new ArrayList<>();

        final CapturingAppender appender = CapturingAppender.attachTo(BoxClient.class, warnings);
        try {
            client.getFiles(new RecordingFolder("root", List.of(fileInfo("f1"))), TEST_FIELDS, f -> {
                attempts.incrementAndGet();
                throw new BoxAPIResponseException("unauthorized", 401, "", null);
            });
        } finally {
            appender.detach();
        }

        assertEquals("the item must be attempted max_retry_count times", 3, attempts.get());
        // Only between attempts: the last failure has no attempt after it to prepare for.
        assertEquals("the connection must be rebuilt between attempts, not after the last one", 2, client.reconnects);
        assertEquals("every retry must be preceded by a wait", 2, client.sleeps);
        assertEquals("giving up must be logged exactly once", 1, warnings.size());
        assertTrue("the warning must name the item that was skipped: " + warnings, warnings.get(0).contains("f1"));
    }

    @Test
    public void test_getFiles_success_neitherSleepsNorWarns() {
        // The complement: the backoff and the give-up warning must not fire on the happy path.
        final RetryRecordingBoxClient client = new RetryRecordingBoxClient();
        client.maxRetryCount = 3;
        final List<String> fileIds = new ArrayList<>();
        final List<String> warnings = new ArrayList<>();

        final CapturingAppender appender = CapturingAppender.attachTo(BoxClient.class, warnings);
        try {
            client.getFiles(new RecordingFolder("root", List.of(fileInfo("f1"))), TEST_FIELDS, f -> fileIds.add(f.getID()));
        } finally {
            appender.detach();
        }

        assertEquals(List.of("f1"), fileIds);
        assertEquals("a successful item must not wait", 0, client.sleeps);
        assertEquals("a successful item must not rebuild the connection", 0, client.reconnects);
        assertEquals("a successful item must not warn: " + warnings, 0, warnings.size());
    }

    /** Collects the formatted message of every WARN event a logger emits while attached. */
    static class CapturingAppender extends AbstractAppender {
        private final List<String> messages;
        private final Logger target;

        private CapturingAppender(final Logger target, final List<String> messages) {
            super("BoxClientTestCapture", null, null, true, Property.EMPTY_ARRAY);
            this.target = target;
            this.messages = messages;
        }

        static CapturingAppender attachTo(final Class<?> clazz, final List<String> messages) {
            final Logger target = (Logger) LogManager.getLogger(clazz);
            final CapturingAppender appender = new CapturingAppender(target, messages);
            appender.start();
            target.addAppender(appender);
            return appender;
        }

        void detach() {
            target.removeAppender(this);
            stop();
        }

        @Override
        public void append(final LogEvent event) {
            if (Level.WARN.equals(event.getLevel())) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }
    }

    // --- forUser / createConnection: per-user identity isolation ---

    /**
     * A {@link BoxClient} whose {@link #newConnection()} hands out an offline, recording
     * connection instead of performing a JWT token exchange with Box. This is the only seam the
     * identity wiring - which connection each client gets, and whether impersonation survives a
     * rebuild - can be observed through without a live tenant.
     */
    static class SeamBoxClient extends BoxClient {
        final List<AsUserRecordingConnection> createdConnections = new ArrayList<>();

        @Override
        protected BoxAPIConnection newConnection() {
            final AsUserRecordingConnection con = new AsUserRecordingConnection();
            createdConnections.add(con);
            return con;
        }
    }

    /** A connection that records every {@code asUser()} call made on it. */
    static class AsUserRecordingConnection extends BoxAPIConnection {
        final List<String> asUserCalls = new ArrayList<>();

        AsUserRecordingConnection() {
            super("dummy-token");
        }

        @Override
        public void asUser(final String userId) {
            asUserCalls.add(userId);
            super.asUser(userId);
        }

        @Override
        public void revokeToken() {
            // The real implementation issues a live HTTP request to Box; these tests must stay
            // offline, and close() is called on the service-account client below.
        }
    }

    /**
     * Reads another client's {@code maxCachedContentSize}.
     *
     * <p>The field is {@code protected} on {@link AbstractCrawlerClient}, in a different package,
     * and {@code AbstractCrawlerClient} declares only a setter - so neither this test class nor a
     * {@link BoxClient} subclass can read it off an instance typed as {@code BoxClient}. Reading
     * it reflectively keeps the assertion here instead of adding a production accessor that only
     * a test would ever call.</p>
     */
    private static long maxCachedContentSizeOf(final BoxClient client) {
        try {
            final java.lang.reflect.Field field = AbstractCrawlerClient.class.getDeclaredField("maxCachedContentSize");
            field.setAccessible(true);
            return field.getLong(client);
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to read maxCachedContentSize", e);
        }
    }

    private SeamBoxClient newSeamClient() {
        final SeamBoxClient client = new SeamBoxClient();
        final Map<String, Object> params = new HashMap<>();
        params.put("max_retry_attempts", "7");
        client.setInitParameterMap(params);
        client.baseUrl = "https://app.box.com";
        client.boxConfig = new BoxConfig("cid", "secret", "ent", new JWTEncryptionPreferences());
        client.maxRetryCount = 4;
        client.accessTokenCache = new InMemoryLRUAccessTokenCache(8);
        client.setMaxCachedContentSize(12345L);
        return client;
    }

    @Test
    public void test_forUser_copiesConnectionStateAndImpersonatesOnItsOwnConnection() {
        final SeamBoxClient client = newSeamClient();

        final BoxClient userClient = client.forUser("user-1");

        assertNotSame("each user must get its own client", client, userClient);
        assertSame("the per-user client must reuse the app configuration", client.boxConfig, userClient.boxConfig);
        assertSame("the per-user client must share the token cache, so impersonating costs no extra token exchange",
                client.accessTokenCache, userClient.accessTokenCache);
        assertEquals("the per-user client must inherit max_retry_count", client.maxRetryCount, userClient.maxRetryCount);
        assertEquals("init() is never called on the per-user client, so maxCachedContentSize must be copied explicitly", 12345L,
                maxCachedContentSizeOf(userClient));
        assertEquals("the per-user client must remember whom it impersonates", "user-1", userClient.impersonatedUserId);

        assertEquals("forUser must open a connection of its own", 1, client.createdConnections.size());
        final AsUserRecordingConnection userConnection = client.createdConnections.get(0);
        assertNotSame("the per-user connection must not be the shared one", client.connection, userClient.connection);
        assertSame(userConnection, userClient.connection);
        assertEquals("the per-user connection must be impersonating that user", List.of("user-1"), userConnection.asUserCalls);
        assertEquals("the shared connection settings must reach the per-user connection too", 7,
                userClient.connection.getMaxRetryAttempts());
    }

    @Test
    public void test_createConnection_rebuildReAppliesImpersonation() {
        // The 401 retry loop rebuilds the connection mid-crawl. If the rebuild dropped the
        // As-User header, every request after it would silently run as the enterprise service
        // account - a per-user crawl indexing files under the wrong identity.
        final SeamBoxClient userClient = newSeamClient();
        userClient.impersonatedUserId = "user-1";

        userClient.createConnection();

        assertEquals(1, userClient.createdConnections.size());
        assertEquals("a rebuilt connection must re-apply the impersonation", List.of("user-1"),
                userClient.createdConnections.get(0).asUserCalls);
        assertSame(userClient.createdConnections.get(0), userClient.connection);
        assertNull("a per-user client must not schedule its own token refresh task", userClient.refreshTokenTask);
    }

    @Test
    public void test_createConnection_serviceAccountIsNotImpersonated() {
        // The complement: the service-account client must stay itself.
        final SeamBoxClient client = newSeamClient();

        client.createConnection();

        try {
            assertEquals(1, client.createdConnections.size());
            assertEquals("the service-account connection must never be impersonated", List.of(),
                    client.createdConnections.get(0).asUserCalls);
            assertNotNull("the service-account client keeps the belt-and-braces refresh timer", client.refreshTokenTask);
        } finally {
            client.close();
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

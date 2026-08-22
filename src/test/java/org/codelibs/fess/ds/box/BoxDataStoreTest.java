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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.box.UnitDsTestCase;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxFile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BoxDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(BoxDataStoreTest.class);

    private BoxDataStore dataStore;

    @Override
    public String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    public boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new BoxDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("Box", dataStore.getName());
    }

    @Test
    public void test_Config_defaultValues() {
        final DataStoreParams paramMap = new DataStoreParams();
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        // Verify that config was created successfully with defaults
        assertTrue(config.toString().contains("maxSize=10000000"));
        assertTrue(config.toString().contains("ignoreError=true"));
        assertTrue(config.toString().contains("ignoreFolder=true"));
        assertTrue(config.toString().contains("supportedMimeTypes=[.*]"));
    }

    @Test
    public void test_Config_customMaxSize() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "20000000");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("maxSize=20000000"));
    }

    @Test
    public void test_Config_invalidMaxSize() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "invalid");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        // Should fall back to default
        assertTrue(config.toString().contains("maxSize=10000000"));
    }

    @Test
    public void test_Config_customFields() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "id,name,size");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("fields=[id, name, size]"));
    }

    @Test
    public void test_Config_ignoreError() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_error", "false");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("ignoreError=false"));
    }

    @Test
    public void test_Config_ignoreFolder() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("ignoreFolder=false"));
    }

    @Test
    public void test_Config_supportedMimeTypes() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("supported_mimetypes", "application/pdf,text/plain");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("supportedMimeTypes=[application/pdf, text/plain]"));
    }

    @Test
    public void test_Config_defaultAwaitTimeout() {
        final DataStoreParams paramMap = new DataStoreParams();
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("awaitTimeout=60}"));
    }

    @Test
    public void test_Config_customAwaitTimeout() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("thread_pool_await_timeout", "120");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("awaitTimeout=120}"));
    }

    @Test
    public void test_Config_invalidAwaitTimeout() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("thread_pool_await_timeout", "invalid");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        // Should fall back to default
        assertTrue(config.toString().contains("awaitTimeout=60}"));
    }

    @Test
    public void test_Config_zeroOrNegativeAwaitTimeout() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();

        final DataStoreParams zeroParamMap = new DataStoreParams();
        zeroParamMap.put("thread_pool_await_timeout", "0");
        // 0 would make awaitTermination() return immediately, dropping every queued file.
        // awaitTimeout is the last field in toString(), so anchor on the closing brace to
        // avoid "1" spuriously matching a value like "10".
        assertTrue(testDataStore.createConfig(zeroParamMap).toString().contains("awaitTimeout=1}"));

        final DataStoreParams negativeParamMap = new DataStoreParams();
        negativeParamMap.put("thread_pool_await_timeout", "-5");
        assertTrue(testDataStore.createConfig(negativeParamMap).toString().contains("awaitTimeout=1}"));
    }

    @Test
    public void test_defaultFields_containsEveryMappedField() {
        final List<String> fields = Arrays.asList(BoxDataStore.DEFAULT_FIELDS);
        // Every field storeFile maps must be requested. Box returns only the
        // fields asked for, so dropping one turns its mapped value null with
        // no error to show for it.
        for (final String required : new String[] { "type", "id", "etag", "sha1", "name", "description", "size", "path_collection",
                "created_at", "modified_at", "trashed_at", "purged_at", "content_created_at", "content_modified_at", "created_by",
                "modified_by", "owned_by", "shared_link", "parent", "item_status", "sequence_id", "file_version", "version_number",
                "comment_count", "permissions", "tags", "lock", "extension", "is_package", "has_collaborations", "watermark_info",
                "collections", "representations" }) {
            assertTrue(required + " must be requested via fields", fields.contains(required));
        }
    }

    @Test
    public void test_getBaseUrl() {
        final MockBoxClient mockClient = new MockBoxClient();
        mockClient.setBaseUrl("https://app.box.com");

        assertEquals("https://app.box.com", mockClient.getBaseUrl());

        mockClient.setBaseUrl("https://custom.box.com");
        assertEquals("https://custom.box.com", mockClient.getBaseUrl());
    }

    @Test
    public void test_getBoxNoteContents() throws Exception {
        final String jsonContent = "{\"atext\":{\"text\":\"This is a test box note content\"}}";
        final InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8));

        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final String content = testDataStore.callGetBoxNoteContents(inputStream);

        assertEquals("This is a test box note content", content);
    }

    @Test
    public void test_getBoxNoteContents_proseMirror() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"paragraph","content":[{"type":"text","text":"new format"}]}
                ]}}
                """;
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            assertEquals("new format", new TestableBoxDataStore().callGetBoxNoteContents(in));
        }
    }

    @Test
    public void test_newFixedThreadPool() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final java.util.concurrent.ExecutorService executor = testDataStore.callNewFixedThreadPool(4);

        assertNotNull(executor);
        executor.shutdown();
    }

    @Test
    public void test_getNumberOfThreads_clampsToAvailableProcessors() {
        final int max = Runtime.getRuntime().availableProcessors() * 2;
        final DataStoreParams paramMap = new DataStoreParams();

        paramMap.put("number_of_threads", "1");
        assertEquals(1, dataStore.getNumberOfThreads(paramMap));

        paramMap.put("number_of_threads", String.valueOf(max + 100));
        assertEquals(max, dataStore.getNumberOfThreads(paramMap));

        paramMap.put("number_of_threads", "0");
        assertEquals(1, dataStore.getNumberOfThreads(paramMap));

        paramMap.put("number_of_threads", "-5");
        assertEquals(1, dataStore.getNumberOfThreads(paramMap));

        paramMap.put("number_of_threads", "not a number");
        assertEquals(1, dataStore.getNumberOfThreads(paramMap));
    }

    @Test
    public void test_storeData() {
        // need src/test/resources/config.json
        final Map<String, String> config = getConfig();
        if (config == null) {
            return;
        }

        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");
        ComponentUtil.register(new ExtractorFactory(), "extractorFactory");
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");

        final DataConfig dataConfig = new DataConfig();
        final DataStoreParams paramMap = new DataStoreParams();
        config.entrySet().stream().forEach(e -> paramMap.put(e.getKey(), e.getValue()));
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

    @Test
    public void test_createResultMap_redactsCredentials() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("client_id", "the-client-id");
        paramMap.put("client_secret", "the-client-secret");
        paramMap.put("public_key_id", "the-public-key-id");
        paramMap.put("private_key", "-----BEGIN ENCRYPTED PRIVATE KEY-----");
        paramMap.put("passphrase", "the-passphrase");
        paramMap.put("enterprise_id", "the-enterprise-id");
        paramMap.put("proxy_password", "the-proxy-password");
        paramMap.put("max_size", "12345");

        final Map<String, Object> resultMap = dataStore.createResultMap(paramMap);

        assertNull(resultMap.get("client_id"));
        assertNull(resultMap.get("client_secret"));
        assertNull(resultMap.get("public_key_id"));
        assertNull(resultMap.get("private_key"));
        assertNull(resultMap.get("passphrase"));
        assertNull(resultMap.get("enterprise_id"));
        assertNull(resultMap.get("proxy_password"));
        assertEquals("12345", resultMap.get("max_size"));
    }

    @Test
    public void test_isEffectiveCollaboration_acceptsOnlyReadableAccepted() {
        assertTrue(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.VIEWER));
        assertTrue(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.EDITOR));
        assertTrue(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.CO_OWNER));
        assertTrue(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.PREVIEWER));

        // Uploader cannot preview or download, so it must not grant search access.
        assertFalse(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.UPLOADER));

        // Pending and rejected collaborators have no access yet.
        assertFalse(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.PENDING, BoxCollaboration.Role.EDITOR));
        assertFalse(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.REJECTED, BoxCollaboration.Role.EDITOR));

        // Missing values must not grant access.
        assertFalse(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(null, BoxCollaboration.Role.EDITOR));
        assertFalse(BoxDataStore.BoxFileAPI.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, null));
    }

    @Test
    public void test_markCrawled_returnsTrueOnlyOnce() {
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();

        assertTrue(dataStore.markCrawled(crawledIds, "111"));
        assertFalse(dataStore.markCrawled(crawledIds, "111"));
        assertFalse(dataStore.markCrawled(crawledIds, "111"));
        assertTrue(dataStore.markCrawled(crawledIds, "222"));
    }

    @Test
    public void test_markCrawled_isThreadSafe() throws Exception {
        final int threads = 16;
        final int rounds = 50;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int round = 0; round < rounds; round++) {
                final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
                final CountDownLatch start = new CountDownLatch(1);
                final AtomicInteger accepted = new AtomicInteger();
                for (int i = 0; i < threads; i++) {
                    pool.execute(() -> {
                        try {
                            start.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (dataStore.markCrawled(crawledIds, "same")) {
                            accepted.incrementAndGet();
                        }
                    });
                }
                start.countDown();
                Thread.sleep(10);
                org.junit.jupiter.api.Assertions.assertEquals(1, accepted.get(), "round " + round + ": exactly one thread must win");
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void test_releaseCrawled_allowsReCrawl() {
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();

        // First crawl succeeds
        assertTrue(dataStore.markCrawled(crawledIds, "111"));
        // Second attempt fails (already claimed)
        assertFalse(dataStore.markCrawled(crawledIds, "111"));
        // Release it
        dataStore.releaseCrawled(crawledIds, "111");
        // Now it can be crawled again
        assertTrue(dataStore.markCrawled(crawledIds, "111"));
    }

    @Test
    public void test_releaseCrawled_isHarmlessIfNeverClaimed() {
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();

        // Release an id that was never claimed — must be harmless
        dataStore.releaseCrawled(crawledIds, "never-claimed");
        // Should be able to claim it normally
        assertTrue(dataStore.markCrawled(crawledIds, "never-claimed"));
    }

    @Test
    public void test_getFileContents_swallowedExtractionFailure_setsContentDegraded() {
        // ignoreError=true makes the guard short-circuit before FessConfig is ever consulted,
        // so this drives the real catch branch of getFileContents without a container.
        final BoxFile file = new BoxFile(null, "file-1");
        final BoxFile.Info info = file.new Info("{\"name\":\"test.txt\"}");
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();
        final ThrowingContentBoxClient client = new ThrowingContentBoxClient();

        final String content =
                dataStore.getFileContents(client, file, info, "https://app.box.com/download/file-1", "text/plain", true, quality);

        assertEquals("", content);
        assertTrue("a swallowed extraction failure must degrade the document's content", quality.contentDegraded);
    }

    @Test
    public void test_getFileContents_genuinelyEmptyBoxNote_leavesContentDegradedFalse() throws Exception {
        // A .boxnote whose payload has neither "doc" nor "atext" parses to an empty string
        // without throwing: this is the constraint that stops every empty file being
        // reprocessed once per collaborator.
        final BoxFile file = new BoxFile(null, "file-1");
        final BoxFile.Info info = file.new Info("{\"name\":\"empty.boxnote\"}");
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();
        final EmptyBoxNoteBoxClient client = new EmptyBoxNoteBoxClient();

        final String content =
                dataStore.getFileContents(client, file, info, "https://app.box.com/download/file-1", "text/plain", true, quality);

        assertEquals("", content);
        assertFalse("a genuinely empty extraction must not be treated as degraded", quality.contentDegraded);
    }

    @Test
    public void test_storeFile_swallowedContentFailure_indexesDegradedDocumentAndReleasesClaim() {
        // Reaching storeFile's success path needs a few container components that convention.xml
        // does not auto-provide in this narrow test classpath (confirmed by probing
        // ComponentUtil.getComponent(MimeTypeHelper.class) and ComponentUtil.getCrawlerStatsHelper(),
        // both of which throw ComponentNotFoundException here). FessConfig, used at the baseRoles
        // line, *is* auto-provided (FessConfigImpl loads fess_config.properties bundled in the fess
        // jar dependency). MimeTypeHelper is avoided by overriding getFileMimeType in
        // RecordingBoxDataStore; CrawlerStatsHelper and SystemHelper (CrawlerStatsHelper.done()'s
        // error-logging path calls ComponentUtil.getSystemHelper() for a timestamp) are registered
        // as bare instances - neither needs its @PostConstruct init() for what this test exercises.
        // Everything else - Config, BoxAclResolver, the release wiring itself - is real, unstubbed
        // production code.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");

        final RecordingBoxDataStore recordingStore = new RecordingBoxDataStore();
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
        recordingStore.markCrawled(crawledIds, "file-1");

        final String infoJson = "{\"type\":\"file\",\"id\":\"file-1\",\"name\":\"test.txt\",\"size\":100,"
                + "\"has_collaborations\":false,\"path_collection\":{\"total_count\":0,\"entries\":[]}}";
        final FakeBoxFile file = new FakeBoxFile("file-1", infoJson);
        final ThrowingContentBoxClient client = new ThrowingContentBoxClient();
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final DataConfig dataConfig = new DataConfig();
        final DataStoreParams paramMap = new DataStoreParams();
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        final List<Map<String, Object>> stored = new ArrayList<>();
        final IndexUpdateCallback callback = new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
                stored.add(dataMap);
            }
        };

        recordingStore.storeFile(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client, aclResolver, crawledIds, file);

        assertEquals("a degraded document must still be indexed, not dropped", 1, stored.size());
        assertFalse("a degraded outcome must release the claim so a later collaborator can retry", crawledIds.contains("file-1"));
        assertEquals("releaseCrawled must be called for the degraded file", List.of("file-1"), recordingStore.releasedIds);
    }

    private Map<String, String> getConfig() {
        final URL url = getClass().getClassLoader().getResource("config.json");
        if (url == null) {
            return null;
        }
        final File file = new File(url.getFile());
        final ObjectMapper mapper = new ObjectMapper();
        final Map<String, String> config = new LinkedHashMap<>();
        try {
            final JsonNode root = mapper.readTree(file);
            final JsonNode boxAppSettings = root.get("boxAppSettings");
            config.put(BoxClient.CLIENT_ID_PARAM, boxAppSettings.get("clientID").asText());
            config.put(BoxClient.CLIENT_SECRET_PARAM, boxAppSettings.get("clientSecret").asText());
            final JsonNode appAuth = boxAppSettings.get("appAuth");
            config.put(BoxClient.PUBLIC_KEY_ID_PARAM, appAuth.get("publicKeyID").asText());
            config.put(BoxClient.PRIVATE_KEY_PARAM, appAuth.get("privateKey").asText());
            config.put(BoxClient.PASSPHRASE_PARAM, appAuth.get("passphrase").asText());
            config.put(BoxClient.ENTERPRISE_ID_PARAM, root.get("enterpriseID").asText());
        } catch (final IOException e) {
            return null;
        }
        return config;
    }

    static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(DataStoreParams paramMap, Map<String, Object> dataMap);

        @Override
        public void store(DataStoreParams paramMap, Map<String, Object> dataMap) {
            final long startTime = System.currentTimeMillis();
            test(paramMap, dataMap);
            executeTime += System.currentTimeMillis() - startTime;
            documentSize++;
        }

        @Override
        public long getDocumentSize() {
            return documentSize;
        }

        @Override
        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public void commit() {
        }
    }

    /**
     * Testable subclass that exposes protected methods for testing
     */
    static class TestableBoxDataStore extends BoxDataStore {
        public Object createConfig(final DataStoreParams paramMap) {
            return new Config(paramMap);
        }

        public String callGetBoxNoteContents(final InputStream in) throws IOException {
            return getBoxNoteContents(in);
        }

        public java.util.concurrent.ExecutorService callNewFixedThreadPool(final int nThreads) {
            return newFixedThreadPool(nThreads);
        }
    }

    /**
     * Mock BoxClient for testing
     */
    static class MockBoxClient extends BoxClient {
        private String baseUrl = "https://app.box.com";

        public void setBaseUrl(final String url) {
            this.baseUrl = url;
        }

        @Override
        public String getBaseUrl() {
            return baseUrl;
        }
    }

    /**
     * A BoxClient whose file download always fails, driving the real (swallowed, since
     * ignore_error defaults to true) catch branch of {@link BoxDataStore#getFileContents}.
     */
    static class ThrowingContentBoxClient extends MockBoxClient {
        @Override
        public InputStream getFileInputStream(final BoxFile file) {
            throw new CrawlingAccessException("simulated download failure");
        }
    }

    /**
     * A BoxClient that returns a Box Note payload with neither "doc" nor "atext" - the
     * genuinely-empty case that {@link BoxNoteParser#parse} handles without throwing.
     */
    static class EmptyBoxNoteBoxClient extends MockBoxClient {
        @Override
        public InputStream getFileInputStream(final BoxFile file) {
            return new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * A BoxFile whose info and download URL are canned, so {@link BoxDataStore#storeFile} can run
     * end-to-end without any real Box API access.
     */
    static class FakeBoxFile extends BoxFile {
        private final String infoJson;

        FakeBoxFile(final String id, final String infoJson) {
            super(null, id);
            this.infoJson = infoJson;
        }

        @Override
        public BoxFile.Info getInfo(final String... fields) {
            return this.new Info(infoJson);
        }

        @Override
        public URL getDownloadURL() {
            try {
                return new URL("https://app.box.com/download/" + getID());
            } catch (final MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public com.box.sdk.BoxResourceIterable<BoxCollaboration.Info> getAllFileCollaborations(final String... fields) {
            // BoxDataStore.BoxFileAPI's constructor only calls this when debug logging is
            // enabled (it is, in this test environment); the real BoxFile implementation needs
            // a live BoxAPIConnection, which this fake file has none of.
            return null;
        }
    }

    /**
     * A BoxDataStore that avoids the MimeTypeHelper container dependency (unrelated to the
     * degradation/release wiring under test) and records every {@link #releaseCrawled} call.
     */
    static class RecordingBoxDataStore extends BoxDataStore {
        final List<String> releasedIds = new ArrayList<>();

        @Override
        protected String getFileMimeType(final BoxFile.Info info) {
            return "text/plain";
        }

        @Override
        protected void releaseCrawled(final Set<String> crawledIds, final String fileId) {
            releasedIds.add(fileId);
            super.releaseCrawled(crawledIds, fileId);
        }
    }

}

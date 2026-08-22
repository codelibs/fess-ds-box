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
import java.util.Collection;
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
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.box.UnitDsTestCase;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxUser;
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
    public void test_getPath_includesFileName() {
        // include_pattern / exclude_pattern are matched against this value, so the
        // file name has to be part of it or no pattern can ever select by name.
        assertEquals("All Files/Projects/report.pdf", BoxDataStore.buildPath(List.of("All Files", "Projects"), "report.pdf"));
        assertEquals("report.pdf", BoxDataStore.buildPath(List.of(), "report.pdf"));
        assertEquals("report.pdf", BoxDataStore.buildPath(null, "report.pdf"));
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
    public void test_Config_ignoreFolder_defaultsTrue() {
        // A test that only asserts toString() passes whether or not the field is actually
        // wired to anything, so assert the field itself.
        final DataStoreParams paramMap = new DataStoreParams();
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final BoxDataStore.Config config = (BoxDataStore.Config) testDataStore.createConfig(paramMap);

        assertTrue(config.ignoreFolder);
    }

    @Test
    public void test_Config_ignoreFolder() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final BoxDataStore.Config config = (BoxDataStore.Config) testDataStore.createConfig(paramMap);

        assertFalse(config.ignoreFolder);
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
        final String fileKey = BoxDataStore.itemKey(BoxClient.ITEM_TYPE_FILE, "file-1");
        recordingStore.markCrawled(crawledIds, fileKey);

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
        assertFalse("a degraded outcome must release the claim so a later collaborator can retry", crawledIds.contains(fileKey));
        assertEquals("releaseCrawled must be called for the degraded file", List.of(fileKey), recordingStore.releasedIds);
    }

    // --- matchesUrlFilter: the shared skip logic buildFileMap and buildFolderMap both use ---

    @Test
    public void test_matchesUrlFilter_nullFilter_alwaysMatches() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();

        assertTrue(testDataStore.callMatchesUrlFilter(null, "All Files/Projects/report.pdf"));
    }

    @Test
    public void test_matchesUrlFilter_filterMismatch_returnsFalse() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final SimpleUrlFilter filter = new SimpleUrlFilter(false);

        assertFalse(testDataStore.callMatchesUrlFilter(filter, "All Files/Projects/report.pdf"));
    }

    @Test
    public void test_matchesUrlFilter_filterMatch_returnsTrue() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final SimpleUrlFilter filter = new SimpleUrlFilter(true);

        assertTrue(testDataStore.callMatchesUrlFilter(filter, "All Files/Projects/report.pdf"));
    }

    // --- buildFolderMap: ignore_folder=false's per-folder document ---

    @Test
    public void test_buildFolderMap_urlUsesFolderShapeAndOmitsFileOnlyFields() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        // A real two-level path_collection (not empty): buildFolderMap must not choke on it, and
        // it is the same fixture test_getPath_folder_pathCollectionHoldsAncestorsOnly uses to pin
        // that path_collection holds ancestors only, never the folder itself.
        final String infoJson = "{\"type\":\"folder\",\"id\":\"folder-1\",\"name\":\"Projects\","
                + "\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"0\",\"name\":\"All Files\"}]}}";
        final FakeBoxFolder folder = new FakeBoxFolder("folder-1", infoJson);
        // Not a plain MockBoxClient: the base BoxClient.getFolderCollaborations() would build a
        // real, network-backed BoxFolder and fail noisily. This keeps the ACL side effect-free
        // since roles are not what this test is checking.
        final BoxClient client = new MockBoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                return List.of();
            }
        };
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final BoxDataStore.Config config = new BoxDataStore.Config(new DataStoreParams());
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        final Map<String, Object> fileMap = testDataStore.callBuildFolderMap(config, client, aclResolver, new HashMap<>(), quality, folder);

        assertNotNull(fileMap);
        assertEquals("https://app.box.com/folder/folder-1", fileMap.get("url"));
        assertEquals("", fileMap.get("contents"));
        assertEquals("folder", fileMap.get("type"));
        assertEquals("folder-1", fileMap.get("id"));
        assertEquals("Projects", fileMap.get("name"));
        // A folder genuinely has no content, size, sha1 or download URL - these keys must be
        // absent, not merely null-valued by accident.
        assertFalse(fileMap.containsKey("sha1"));
        assertFalse(fileMap.containsKey("download_url"));
        assertFalse(fileMap.containsKey("size"));
        assertFalse(fileMap.containsKey("mimetype"));
        assertFalse(fileMap.containsKey("filetype"));
    }

    @Test
    public void test_getPath_folder_pathCollectionHoldsAncestorsOnly() {
        // Pins the shape include_pattern/exclude_pattern match a folder document against:
        // path_collection holds ancestors only, never the folder itself. If Box ever included the
        // folder in its own path_collection, getPath would emit "All Files/Projects/Projects"
        // instead of "All Files/Projects", and this test would catch it.
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final String infoJson = "{\"type\":\"folder\",\"id\":\"folder-1\",\"name\":\"Projects\","
                + "\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"0\",\"name\":\"All Files\"}]}}";
        final FakeBoxFolder folder = new FakeBoxFolder("folder-1", infoJson);

        assertEquals("All Files/Projects", testDataStore.callGetPath(folder.getInfo()));
    }

    @Test
    public void test_defaultFolderFields_excludesFileOnlyNames() {
        // getChildren(fields) tolerating DEFAULT_FIELDS' file-only names is not evidence that a
        // single-folder GET /folders/{id} does too - getChildren legitimately spans both item
        // types, a single-folder fetch does not.
        final List<String> folderFields = Arrays.asList(BoxDataStore.DEFAULT_FOLDER_FIELDS);
        for (final String fileOnly : new String[] { "sha1", "file_version", "version_number", "comment_count", "lock", "extension",
                "is_package", "representations" }) {
            assertFalse(fileOnly + " is file-only and must not be requested for a folder", folderFields.contains(fileOnly));
        }
        // The four fields BoxAclResolver's folder role resolution needs must still be requested.
        for (final String needed : new String[] { "has_collaborations", "path_collection", "owned_by", "shared_link" }) {
            assertTrue(needed + " is required by BoxAclResolver and must still be requested for a folder", folderFields.contains(needed));
        }
    }

    @Test
    public void test_buildFolderMap_requestsFolderFieldsNotConfigFields() {
        // config.fields defaults to (file-shaped) DEFAULT_FIELDS; buildFolderMap must not pass
        // that straight through to folder.getInfo(...).
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final String infoJson = "{\"type\":\"folder\",\"id\":\"folder-1\",\"name\":\"Projects\","
                + "\"path_collection\":{\"total_count\":0,\"entries\":[]}}";
        final FakeBoxFolder folder = new FakeBoxFolder("folder-1", infoJson);
        final BoxClient client = new MockBoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                return List.of();
            }
        };
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final BoxDataStore.Config config = new BoxDataStore.Config(new DataStoreParams());
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        testDataStore.callBuildFolderMap(config, client, aclResolver, new HashMap<>(), quality, folder);

        assertEquals(Arrays.asList(BoxDataStore.DEFAULT_FOLDER_FIELDS), Arrays.asList(folder.lastRequestedFields));
    }

    @Test
    public void test_buildFolderMap_rolesGoThroughAclResolverFolderOverload() {
        // Proves storeFolder gives folders the same role treatment as files: default_permissions,
        // the defaultDataMap role merge, and BoxAclResolver's folder-specific ACL resolution.
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final String infoJson = "{\"type\":\"folder\",\"id\":\"folder-1\",\"name\":\"Projects\","
                + "\"path_collection\":{\"total_count\":0,\"entries\":[]}}";
        final FakeBoxFolder folder = new FakeBoxFolder("folder-1", infoJson);
        final BoxClient client = new MockBoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                return List.of();
            }
        };
        // default_permissions is set directly on the resolver rather than through paramMap: the
        // Config path (Config.getDefaultPermissions) needs a PermissionHelper component that
        // this narrow test classpath does not provide. What is under test here is that
        // buildFolderMap passes whatever the resolver was built with through to the folder's
        // roles, exactly as it does for files - not Config's own already-file-proven wiring.
        final BoxDataStore.Config config = new BoxDataStore.Config(new DataStoreParams());
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of("Rfolder-default-role"), null);
        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(ComponentUtil.getFessConfig().getIndexFieldRole(), List.of("Rfrom-default-data-map"));
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        final Map<String, Object> fileMap = testDataStore.callBuildFolderMap(config, client, aclResolver, defaultDataMap, quality, folder);

        @SuppressWarnings("unchecked")
        final List<String> roles = (List<String>) fileMap.get("roles");
        assertTrue("default_permissions must reach the folder document", roles.contains("Rfolder-default-role"));
        assertTrue("the defaultDataMap role merge must reach the folder document", roles.contains("Rfrom-default-data-map"));
    }

    @Test
    public void test_storeFolder_swallowedAclFailure_indexesDegradedDocumentAndReleasesClaim() {
        // Mirrors test_storeFile_swallowedContentFailure_indexesDegradedDocumentAndReleasesClaim:
        // proves storeFolder shares storeItem's claim/degradation wiring with storeFile, not a
        // reimplementation of it.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        final RecordingBoxDataStore recordingStore = new RecordingBoxDataStore();
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
        final String folderKey = BoxDataStore.itemKey(BoxClient.ITEM_TYPE_FOLDER, "folder-1");
        recordingStore.markCrawled(crawledIds, folderKey);

        final String infoJson = "{\"type\":\"folder\",\"id\":\"folder-1\",\"name\":\"Projects\","
                + "\"path_collection\":{\"total_count\":0,\"entries\":[]}}";
        final FakeBoxFolder folder = new FakeBoxFolder("folder-1", infoJson);
        // A client whose folder collaboration lookup always fails, driving BoxAclResolver's
        // swallowed-failure path (quality.aclDegraded = true) for the folder's own roles.
        final BoxClient client = new MockBoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                throw new RuntimeException("simulated folder collaboration lookup failure");
            }
        };
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

        recordingStore.storeFolder(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client, aclResolver, crawledIds,
                folder);

        assertEquals("a degraded folder document must still be indexed, not dropped", 1, stored.size());
        assertFalse("a degraded outcome must release the claim so a later collaborator can retry", crawledIds.contains(folderKey));
        assertEquals("releaseCrawled must be called for the degraded folder", List.of(folderKey), recordingStore.releasedIds);
    }

    // --- crawlFolder: the wiring shared by crawlUserFolders (per user) and crawlRootFolder ---

    @Test
    public void test_crawlFolder_ignoreFolderTrue_passesNullFolderConsumer() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DataStoreParams paramMap = new DataStoreParams();
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap); // ignoreFolder defaults true
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            dataStore.crawlFolder(new DataConfig(), new TestCallback() {
                @Override
                void test(final DataStoreParams p, final Map<String, Object> dataMap) {
                }
            }, config, paramMap, new HashMap<>(), new HashMap<>(), client, new FakeBoxFolder("root", "{}"), aclResolver,
                    ConcurrentHashMap.newKeySet(), executorService, 0L);
        } finally {
            executorService.shutdownNow();
        }

        assertNotNull(client.capturedFileConsumer);
        assertNull("ignore_folder defaults to true: a caller who does not want folders must not receive a folder consumer",
                client.capturedFolderConsumer);
    }

    @Test
    public void test_crawlFolder_ignoreFolderFalse_passesNonNullFolderConsumer() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap);
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            dataStore.crawlFolder(new DataConfig(), new TestCallback() {
                @Override
                void test(final DataStoreParams p, final Map<String, Object> dataMap) {
                }
            }, config, paramMap, new HashMap<>(), new HashMap<>(), client, new FakeBoxFolder("root", "{}"), aclResolver,
                    ConcurrentHashMap.newKeySet(), executorService, 0L);
        } finally {
            executorService.shutdownNow();
        }

        assertNotNull(client.capturedFolderConsumer);
    }

    @Test
    public void test_crawlFolder_fileAndFolderSharingNumericId_bothProcessed() throws InterruptedException {
        // Box does not document that file ids and folder ids are drawn from disjoint spaces -
        // getUrl and BoxAclResolver's javadoc both treat "unique id" as scoped to one item type,
        // never across types. If a file and a folder happened to share a numeric id, the shared
        // crawledIds set must not let one silently drop the other.
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap);
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final RecordingStoreBoxDataStore store = new RecordingStoreBoxDataStore();

        try {
            store.crawlFolder(new DataConfig(), new TestCallback() {
                @Override
                void test(final DataStoreParams p, final Map<String, Object> dataMap) {
                }
            }, config, paramMap, new HashMap<>(), new HashMap<>(), client, new FakeBoxFolder("root", "{}"), aclResolver, crawledIds,
                    executorService, 0L);

            client.capturedFileConsumer.accept(new FakeBoxFile("42", "{\"type\":\"file\",\"id\":\"42\"}"));
            client.capturedFolderConsumer.accept(new FakeBoxFolder("42", "{}"));
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("the file with id 42 must be processed", List.of("42"), store.storedFileIds);
        assertEquals("the folder with id 42 must ALSO be processed, not silently dropped by a colliding dedup key", List.of("42"),
                store.storedFolderIds);
    }

    @Test
    public void test_crawlFolder_folderConsumerBody_claimsQueuesThrottlesAndRespectsAlive() throws InterruptedException {
        // Exercises the folder consumer's own body - the alive check, the markCrawled claim, the
        // executor hand-off and the read_interval throttle. No other test invokes
        // capturedFolderConsumer.accept(...): it was previously only ever asserted null/non-null.
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap);
        final BoxAclResolver aclResolver = new BoxAclResolver(List.of(), null);
        final Set<String> crawledIds = ConcurrentHashMap.newKeySet();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final RecordingStoreBoxDataStore store = new RecordingStoreBoxDataStore();

        try {
            store.crawlFolder(new DataConfig(), new TestCallback() {
                @Override
                void test(final DataStoreParams p, final Map<String, Object> dataMap) {
                }
            }, config, paramMap, new HashMap<>(), new HashMap<>(), client, new FakeBoxFolder("root", "{}"), aclResolver, crawledIds,
                    executorService, 5L /* readInterval > 0, so the throttle actually runs */);

            // Phase 1: alive=false must stop the folder from ever reaching the executor.
            store.setAliveForTest(false);
            client.capturedFolderConsumer.accept(new FakeBoxFolder("blocked-by-alive", "{}"));

            // Phase 2: alive=true. The first accept() must claim, queue and throttle; the
            // duplicate second accept() of the same id must be blocked by markCrawled.
            store.setAliveForTest(true);
            client.capturedFolderConsumer.accept(new FakeBoxFolder("folder-1", "{}"));
            client.capturedFolderConsumer.accept(new FakeBoxFolder("folder-1", "{}"));
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("alive=false must block the first folder, and the duplicate second folder-1 must be blocked by markCrawled - "
                + "only one storeFolder call may reach the executor", List.of("folder-1"), store.storedFolderIds);
        assertEquals("the read_interval throttle must run exactly once, for the single accepted folder", 1, store.sleepCalls.get());
    }

    // --- crawlRootFolder / storeData: the root_folder_id dispatch ---

    @Test
    public void test_crawlRootFolder_resolvesFolderByIdAndSharesCrawlFolderWiring() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final ExecutorCapturingBoxDataStore store = new ExecutorCapturingBoxDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        final BoxDataStore.Config config = new BoxDataStore.Config(paramMap);
        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        final IndexUpdateCallback callback = new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
            }
        };

        store.crawlRootFolder(dataConfig, callback, config, paramMap, scriptMap, defaultDataMap, client, "999");

        assertEquals("999", client.capturedFolder.getID());
        assertNotNull(client.capturedFileConsumer);
        assertNull(client.capturedFolderConsumer);
        assertFalse("root_folder_id must not enumerate users", client.getUsersCalled);
        assertNotNull(store.lastExecutorService);
        assertTrue("the executor crawlFolder was given must have been shut down", store.lastExecutorService.isShutdown());
    }

    @Test
    public void test_storeData_rootFolderIdSet_crawlsSingleFolderNotUsers() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DispatchTestBoxDataStore store = new DispatchTestBoxDataStore(client);
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("root_folder_id", "999");

        store.storeData(new DataConfig(), new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
            }
        }, paramMap, new HashMap<>(), new HashMap<>());

        assertEquals("999", client.capturedFolder.getID());
        assertFalse("root_folder_id set must not enumerate users", client.getUsersCalled);
    }

    @Test
    public void test_storeData_rootFolderIdUnset_crawlsUsersNotSingleFolder() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DispatchTestBoxDataStore store = new DispatchTestBoxDataStore(client);
        final DataStoreParams paramMap = new DataStoreParams();

        store.storeData(new DataConfig(), new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
            }
        }, paramMap, new HashMap<>(), new HashMap<>());

        assertTrue("an unset root_folder_id must keep enumerating every user, as before", client.getUsersCalled);
        assertNull("crawlUserFolders never calls the recording client's own getFiles override directly", client.capturedFolder);
    }

    @Test
    public void test_storeData_rootFolderIdBlank_crawlsUsersNotSingleFolder() {
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DispatchTestBoxDataStore store = new DispatchTestBoxDataStore(client);
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("root_folder_id", "   ");

        store.storeData(new DataConfig(), new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
            }
        }, paramMap, new HashMap<>(), new HashMap<>());

        assertTrue("a blank root_folder_id must behave exactly as unset", client.getUsersCalled);
    }

    @Test
    public void test_storeData_rootFolderIdWithSurroundingWhitespace_isTrimmedBeforeLookup() {
        // The blank test above ("   ") already passes even without trim(), since
        // StringUtil.isNotBlank rejects it either way. What trim() actually protects is a
        // non-blank value with stray whitespace reaching client.getFolder(" 999 ") untrimmed.
        final RecordingGetFilesBoxClient client = new RecordingGetFilesBoxClient();
        final DispatchTestBoxDataStore store = new DispatchTestBoxDataStore(client);
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("root_folder_id", "  999  ");

        store.storeData(new DataConfig(), new TestCallback() {
            @Override
            void test(final DataStoreParams p, final Map<String, Object> dataMap) {
            }
        }, paramMap, new HashMap<>(), new HashMap<>());

        assertEquals("999", client.capturedFolder.getID());
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

        public boolean callMatchesUrlFilter(final UrlFilter urlFilter, final String path) {
            return matchesUrlFilter(urlFilter, path);
        }

        public Map<String, Object> callBuildFolderMap(final Config config, final BoxClient client, final BoxAclResolver aclResolver,
                final Map<String, Object> defaultDataMap, final DocumentQuality quality, final BoxFolder folder) {
            return buildFolderMap(config, client, aclResolver, defaultDataMap, quality, folder);
        }

        public String callGetPath(final com.box.sdk.BoxItem.Info info) {
            return getPath(info);
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

    /**
     * A BoxFolder whose info is canned, so {@link BoxDataStore#buildFolderMap} and
     * {@link BoxDataStore#storeFolder} can run end-to-end without any real Box API access.
     */
    static class FakeBoxFolder extends BoxFolder {
        private final String infoJson;
        /** The fields array the most recent {@link #getInfo} call was actually given. */
        String[] lastRequestedFields;

        FakeBoxFolder(final String id, final String infoJson) {
            super(null, id);
            this.infoJson = infoJson;
        }

        @Override
        public BoxFolder.Info getInfo(final String... fields) {
            lastRequestedFields = fields;
            return this.new Info(infoJson);
        }
    }

    /** A {@link UrlFilter} whose match() result is canned. */
    static class SimpleUrlFilter implements UrlFilter {
        private final boolean matches;

        SimpleUrlFilter(final boolean matches) {
            this.matches = matches;
        }

        @Override
        public void init(final String sessionId) {
        }

        @Override
        public boolean match(final String url) {
            return matches;
        }

        @Override
        public void addInclude(final String urlPattern) {
        }

        @Override
        public void addExclude(final String urlPattern) {
        }

        @Override
        public void processUrl(final String url) {
        }

        @Override
        public void clear() {
        }
    }

    /**
     * A BoxClient whose {@link #getUsers}, {@link #forUser} and
     * {@link #getFiles(BoxFolder, String[], Consumer, Consumer)} are all recorded rather than
     * hitting the network, so {@link BoxDataStore#crawlFolder}, {@link BoxDataStore#crawlRootFolder}
     * and the {@code root_folder_id} branch of {@link BoxDataStore#storeData} can all be driven
     * end-to-end without any real Box API access.
     */
    static class RecordingGetFilesBoxClient extends MockBoxClient {
        BoxFolder capturedFolder;
        String[] capturedFields;
        Consumer<BoxFile> capturedFileConsumer;
        Consumer<BoxFolder> capturedFolderConsumer;
        boolean getUsersCalled;

        @Override
        public void getUsers(final String filterTerm, final Consumer<BoxUser.Info> consumer) {
            getUsersCalled = true;
            // No users to iterate - crawlUserFolders' body must complete without one.
        }

        @Override
        public BoxClient forUser(final String userId) {
            // Not expected to be reached by the tests that use this fake: root_folder_id never
            // impersonates, and the getUsers() override above never yields a user to call this for.
            return this;
        }

        @Override
        public void getFiles(final BoxFolder folder, final String[] fields, final Consumer<BoxFile> fileConsumer,
                final Consumer<BoxFolder> folderConsumer) {
            capturedFolder = folder;
            capturedFields = fields;
            capturedFileConsumer = fileConsumer;
            capturedFolderConsumer = folderConsumer;
        }
    }

    /** A BoxDataStore whose {@link #createClient} returns a pre-built client instead of a real one. */
    static class DispatchTestBoxDataStore extends BoxDataStore {
        private final BoxClient client;

        DispatchTestBoxDataStore(final BoxClient client) {
            this.client = client;
        }

        @Override
        protected BoxClient createClient(final DataStoreParams paramMap) {
            return client;
        }
    }

    /** A BoxDataStore that records the executor {@link #newFixedThreadPool} last created. */
    static class ExecutorCapturingBoxDataStore extends BoxDataStore {
        ExecutorService lastExecutorService;

        @Override
        protected ExecutorService newFixedThreadPool(final int nThreads) {
            lastExecutorService = super.newFixedThreadPool(nThreads);
            return lastExecutorService;
        }
    }

    /**
     * A BoxDataStore that skips the real storeFile/storeFolder pipeline - and the container
     * components it needs - recording instead which item ids reach each, and that skips the real
     * {@link #sleep} so the read_interval throttle can be asserted without an actual delay. Used
     * to test {@link BoxDataStore#crawlFolder}'s own wiring (the alive check, the markCrawled
     * claim, the executor hand-off, and the throttle) in isolation from storeFile/storeFolder's
     * own correctness, which is covered elsewhere.
     */
    static class RecordingStoreBoxDataStore extends BoxDataStore {
        final List<String> storedFileIds = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<String> storedFolderIds = new java.util.concurrent.CopyOnWriteArrayList<>();
        final AtomicInteger sleepCalls = new AtomicInteger();

        @Override
        protected void storeFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
                final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                final BoxClient client, final BoxAclResolver aclResolver, final Set<String> crawledIds, final BoxFile file) {
            storedFileIds.add(file.getID());
        }

        @Override
        protected void storeFolder(final DataConfig dataConfig, final IndexUpdateCallback callback, final Config config,
                final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                final BoxClient client, final BoxAclResolver aclResolver, final Set<String> crawledIds, final BoxFolder folder) {
            storedFolderIds.add(folder.getID());
        }

        @Override
        protected void sleep(final long millis) {
            sleepCalls.incrementAndGet();
        }

        /**
         * Sets {@code alive}, inherited from {@link org.codelibs.fess.ds.AbstractDataStore} in a
         * different package: {@code protected} cross-package access is only legal from within a
         * subclass's own code, so the test class itself cannot reach the field directly and needs
         * this wrapper.
         */
        void setAliveForTest(final boolean value) {
            this.alive = value;
        }
    }

}

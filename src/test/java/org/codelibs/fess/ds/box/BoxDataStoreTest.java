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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.crawler.extractor.ExtractorFactory;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BoxDataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new BoxDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("Box", dataStore.getName());
    }

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

    public void test_Config_customMaxSize() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "20000000");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("maxSize=20000000"));
    }

    public void test_Config_invalidMaxSize() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_size", "invalid");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        // Should fall back to default
        assertTrue(config.toString().contains("maxSize=10000000"));
    }

    public void test_Config_customFields() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "id,name,size");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("fields=[id, name, size]"));
    }

    public void test_Config_ignoreError() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_error", "false");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("ignoreError=false"));
    }

    public void test_Config_ignoreFolder() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_folder", "false");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("ignoreFolder=false"));
    }

    public void test_Config_supportedMimeTypes() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("supported_mimetypes", "application/pdf,text/plain");
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final Object config = testDataStore.createConfig(paramMap);

        assertNotNull(config);
        assertTrue(config.toString().contains("supportedMimeTypes=[application/pdf, text/plain]"));
    }

    public void test_getBaseUrl() {
        final MockBoxClient mockClient = new MockBoxClient();
        mockClient.setBaseUrl("https://app.box.com");

        assertEquals("https://app.box.com", mockClient.getBaseUrl());

        mockClient.setBaseUrl("https://custom.box.com");
        assertEquals("https://custom.box.com", mockClient.getBaseUrl());
    }

    public void test_getBoxNoteContents() throws Exception {
        final String jsonContent = "{\"atext\":{\"text\":\"This is a test box note content\"}}";
        final InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8));

        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final String content = testDataStore.callGetBoxNoteContents(inputStream);

        assertEquals("This is a test box note content", content);
    }

    public void test_newFixedThreadPool() {
        final TestableBoxDataStore testDataStore = new TestableBoxDataStore();
        final java.util.concurrent.ExecutorService executor = testDataStore.callNewFixedThreadPool(4);

        assertNotNull(executor);
        executor.shutdown();
    }

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

}

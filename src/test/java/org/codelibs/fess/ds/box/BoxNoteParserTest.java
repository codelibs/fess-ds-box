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
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BoxNoteParserTest {

    private String parse(final String json) throws Exception {
        return BoxNoteParser.parse(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void test_proseMirror_headingAndParagraph() throws Exception {
        final String json = """
                {"version":1,"schema_version":11,"doc":{"type":"doc","content":[
                  {"type":"heading","attrs":{"level":1},"content":[{"type":"text","text":"Title"}]},
                  {"type":"paragraph","content":[{"type":"text","text":"Hello "},{"type":"text","text":"world"}]}
                ]}}
                """;
        assertEquals("Title\nHello world", parse(json));
    }

    @Test
    public void test_proseMirror_lists() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"bullet_list","content":[
                    {"type":"list_item","content":[{"type":"paragraph","content":[{"type":"text","text":"one"}]}]},
                    {"type":"list_item","content":[{"type":"paragraph","content":[{"type":"text","text":"two"}]}]}
                  ]}
                ]}}
                """;
        assertEquals("one\ntwo", parse(json));
    }

    @Test
    public void test_proseMirror_hardBreak() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"paragraph","content":[
                    {"type":"text","text":"a"},{"type":"hard_break"},{"type":"text","text":"b"}]}
                ]}}
                """;
        assertEquals("a\nb", parse(json));
    }

    @Test
    public void test_proseMirror_table() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"table","content":[
                    {"type":"table_row","content":[
                      {"type":"table_cell","content":[{"type":"paragraph","content":[{"type":"text","text":"c1"}]}]},
                      {"type":"table_cell","content":[{"type":"paragraph","content":[{"type":"text","text":"c2"}]}]}
                    ]}]}
                ]}}
                """;
        assertEquals("c1\nc2", parse(json));
    }

    @Test
    public void test_proseMirror_unknownNodeTypeStillExtractsText() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"future_widget","content":[{"type":"text","text":"kept"}]}
                ]}}
                """;
        assertEquals("kept", parse(json));
    }

    @Test
    public void test_legacy_atext() throws Exception {
        final String json = """
                {"atext":{"text":"legacy body"},"pool":{}}
                """;
        assertEquals("legacy body", parse(json));
    }

    @Test
    public void test_emptyObject_returnsEmptyWithoutThrowing() throws Exception {
        assertEquals("", parse("{}"));
    }

    @Test
    public void test_atextWithoutText_returnsEmptyWithoutThrowing() throws Exception {
        assertEquals("", parse("""
                {"atext":{}}
                """));
    }

    @Test
    public void test_jsonNull_returnsEmpty() throws Exception {
        assertEquals("", parse("null"));
    }

    @Test
    public void test_docWithoutContent_returnsEmpty() throws Exception {
        assertEquals("", parse("""
                {"doc":{"type":"doc"}}
                """));
    }

    @Test
    public void test_japaneseText() throws Exception {
        final String json = """
                {"doc":{"type":"doc","content":[
                  {"type":"paragraph","content":[{"type":"text","text":"日本語の本文"}]}
                ]}}
                """;
        assertNotNull(parse(json));
        assertEquals("日本語の本文", parse(json));
    }
}

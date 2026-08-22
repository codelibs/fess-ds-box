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

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extracts plain text from a Box Note ({@code .boxnote}) document.
 *
 * <p>Box Notes moved to a ProseMirror-based format in August 2022; documents
 * created or edited since then have a {@code doc} node and no {@code atext}.
 * Older notes keep the Etherpad-derived {@code atext} structure. Both are
 * supported here, and an unrecognised payload yields an empty string rather
 * than an exception so that a single odd note cannot fail a crawl.</p>
 */
public final class BoxNoteParser {

    private static final Logger logger = LogManager.getLogger(BoxNoteParser.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** ProseMirror node types that end a line of text. */
    private static final Set<String> BLOCK_TYPES = Set.of("heading", "paragraph", "list_item", "check_list_item", "blockquote",
            "call_out_box", "horizontal_rule", "table_cell", "table_header");

    private BoxNoteParser() {
        // nothing
    }

    /**
     * Parses a Box Note and returns its plain text content.
     *
     * @param in the Box Note content
     * @return the extracted text, or an empty string if the format is not recognised
     * @throws IOException if the stream cannot be read or does not contain JSON
     */
    public static String parse(final InputStream in) throws IOException {
        return parse(MAPPER.readTree(in));
    }

    static String parse(final JsonNode root) {
        if (root == null || root.isNull() || root.isMissingNode()) {
            return StringUtil.EMPTY;
        }

        final JsonNode doc = root.get("doc");
        if (doc != null && !doc.isNull()) {
            final StringBuilder buf = new StringBuilder();
            append(doc, buf);
            return buf.toString().strip();
        }

        final JsonNode atext = root.get("atext");
        if (atext != null && !atext.isNull()) {
            final JsonNode text = atext.get("text");
            if (text != null && text.isTextual()) {
                return text.asText();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("boxnote has atext without text");
            }
            return StringUtil.EMPTY;
        }

        logger.warn("Unrecognised boxnote format. No 'doc' or 'atext' node was found.");
        return StringUtil.EMPTY;
    }

    private static void append(final JsonNode node, final StringBuilder buf) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isArray()) {
            node.forEach(child -> append(child, buf));
            return;
        }

        final JsonNode typeNode = node.get("type");
        final String type = typeNode != null && typeNode.isTextual() ? typeNode.asText() : null;

        if ("text".equals(type)) {
            final JsonNode text = node.get("text");
            if (text != null && text.isTextual()) {
                buf.append(text.asText());
            }
            return;
        }
        if ("hard_break".equals(type)) {
            appendLineBreak(buf);
            return;
        }

        // Unknown types are still traversed so that new Box node types keep working.
        append(node.get("content"), buf);

        if (type != null && BLOCK_TYPES.contains(type)) {
            appendLineBreak(buf);
        }
    }

    private static void appendLineBreak(final StringBuilder buf) {
        if (buf.length() > 0 && buf.charAt(buf.length() - 1) != '\n') {
            buf.append('\n');
        }
    }
}

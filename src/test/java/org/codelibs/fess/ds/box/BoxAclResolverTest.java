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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoxAclResolverTest {

    @Test
    public void test_hasCollaborations_true() {
        assertTrue(BoxAclResolver.hasCollaborations("{\"id\":\"1\",\"has_collaborations\":true}"));
    }

    @Test
    public void test_hasCollaborations_false() {
        assertFalse(BoxAclResolver.hasCollaborations("{\"id\":\"1\",\"has_collaborations\":false}"));
    }

    @Test
    public void test_hasCollaborations_absentFieldIsConservativelyTrue() {
        // The field is only present when requested through "fields"; if it is
        // missing we must not silently skip the per-file lookup.
        assertTrue(BoxAclResolver.hasCollaborations("{\"id\":\"1\"}"));
    }

    @Test
    public void test_hasCollaborations_malformedIsConservativelyTrue() {
        assertTrue(BoxAclResolver.hasCollaborations("not json"));
        assertTrue(BoxAclResolver.hasCollaborations(null));
    }

    /** Resolver with the network-facing parts stubbed out. */
    private static class StubResolver extends BoxAclResolver {
        private final Map<String, List<String>> folderRoles;
        int folderLookups;

        StubResolver(final Map<String, List<String>> folderRoles, final List<String> defaultPermissions,
                final String companySharedLinkRole) {
            super(null, defaultPermissions, companySharedLinkRole);
            this.folderRoles = folderRoles;
        }

        @Override
        protected List<String> loadFolderRoles(final String folderId) {
            folderLookups++;
            return folderRoles.getOrDefault(folderId, List.of());
        }
    }

    @Test
    public void test_getFolderRoles_cachesPerFolder() {
        final StubResolver resolver = new StubResolver(Map.of("100", List.of("1user")), List.of(), null);

        assertEquals(List.of("1user"), resolver.getFolderRoles("100"));
        assertEquals(List.of("1user"), resolver.getFolderRoles("100"));
        assertEquals(List.of("1user"), resolver.getFolderRoles("100"));

        assertEquals(1, resolver.folderLookups, "the folder must only be read once");
    }

    @Test
    public void test_merge_dedupesAndPreservesOrder() {
        final List<String> merged = BoxAclResolver.merge(List.of("a", "b"), List.of("b", "c"), List.of("a"));
        assertEquals(List.of("a", "b", "c"), merged);
    }

    @Test
    public void test_merge_ignoresNullAndBlank() {
        final List<String> merged = BoxAclResolver.merge(List.of("a"), null, List.of("", "  ", "b"));
        assertEquals(List.of("a", "b"), merged);
    }
}

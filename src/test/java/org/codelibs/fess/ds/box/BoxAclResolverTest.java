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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxFile;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoxAclResolverTest {

    /** Builds a {@link BoxFile.Info} from a raw JSON payload without any network access. */
    private static BoxFile.Info info(final String json) {
        return new BoxFile(null, "file-1").new Info(json);
    }

    @Test
    public void test_hasCollaborations_true() {
        assertTrue(BoxAclResolver.hasCollaborations(Boolean.TRUE));
    }

    @Test
    public void test_hasCollaborations_false() {
        assertFalse(BoxAclResolver.hasCollaborations(Boolean.FALSE));
    }

    @Test
    public void test_hasCollaborations_nullIsConservativelyTrue() {
        // The flag is only populated when has_collaborations was requested through
        // "fields"; if it is null we must not silently skip the per-file lookup.
        assertTrue(BoxAclResolver.hasCollaborations(null));
    }

    /** Resolver with the network- and container-facing parts stubbed out. */
    private static class StubResolver extends BoxAclResolver {
        private final Map<String, List<String>> folderRoles;
        List<String> ownerRoles = List.of();
        List<String> fileRoles = List.of();
        int folderLookups;
        int fileCollaborationLookups;

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

        @Override
        protected List<String> getOwnerRoles(final BoxFile.Info info) {
            return ownerRoles;
        }

        @Override
        protected Collection<BoxCollaboration.Info> getFileCollaborations(final BoxFile.Info info) {
            fileCollaborationLookups++;
            return List.of();
        }

        @Override
        protected List<String> toRoles(final Collection<BoxCollaboration.Info> collaborations) {
            return fileRoles;
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
    public void test_getFolderRoles_retriesAfterFailure() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null) {
            private boolean failedOnce;

            @Override
            protected List<String> loadFolderRoles(final String folderId) {
                folderLookups++;
                if (!failedOnce) {
                    failedOnce = true;
                    return null;
                }
                return List.of("1user");
            }
        };

        assertEquals(List.of(), resolver.getFolderRoles("100"), "a failed lookup must not poison the result");
        assertEquals(List.of("1user"), resolver.getFolderRoles("100"), "the retry must return the real roles");
        assertTrue(resolver.folderLookups > 1, "a failed lookup must not be cached, so it is retried");
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

    @Test
    public void test_getOwnerRoles_nullOwner_returnsEmptyWithoutThrowing() {
        final BoxAclResolver resolver = new BoxAclResolver(null, List.of(), null);

        assertEquals(List.of(), resolver.getOwnerRoles(info("{}")));
    }

    @Test
    public void test_getRoles_hasCollaborationsFalse_skipsFileLookup() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        resolver.getRoles(info("{\"has_collaborations\":false}"), List.of());

        assertEquals(0, resolver.fileCollaborationLookups, "the optimisation must skip the per-file lookup");
    }

    @Test
    public void test_getRoles_hasCollaborationsTrue_performsFileLookup() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        resolver.getRoles(info("{\"has_collaborations\":true}"), List.of());

        assertEquals(1, resolver.fileCollaborationLookups);
    }

    @Test
    public void test_getRoles_nullPathCollectionAndOwner_doesNotThrow() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        assertDoesNotThrow(() -> resolver.getRoles(info("{\"has_collaborations\":false}"), List.of()));
    }

    @Test
    public void test_getRoles_skipsRootFolderAncestor() {
        final StubResolver resolver = new StubResolver(Map.of("0", List.of("wronguser"), "100", List.of("gooduser")), List.of(), null);
        final BoxFile.Info info = info(
                "{\"has_collaborations\":false,\"path_collection\":{\"total_count\":2,\"entries\":[{\"id\":\"0\"},{\"id\":\"100\"}]}}");

        final List<String> roles = resolver.getRoles(info, List.of());

        assertEquals(List.of("gooduser"), roles);
        assertEquals(1, resolver.folderLookups, "the root folder id \"0\" must never be looked up");
    }

    @Test
    public void test_getRoles_sharedLinkCompanyAccess_addsConfiguredRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), "shared-role");
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"company\"}}");

        assertTrue(resolver.getRoles(info, List.of()).contains("shared-role"));
    }

    @Test
    public void test_getRoles_sharedLinkNonCompanyAccess_doesNotAddRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), "shared-role");
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"open\"}}");

        assertFalse(resolver.getRoles(info, List.of()).contains("shared-role"));
    }

    @Test
    public void test_getRoles_companySharedLinkRoleBlank_neverAddsRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"company\"}}");

        assertEquals(List.of(), resolver.getRoles(info, List.of()));
    }

    @Test
    public void test_getRoles_mergesAllSourcesPreservingOrderAndDedupingAcrossThem() {
        final StubResolver resolver = new StubResolver(Map.of("100", List.of("folderuser", "shared")), List.of("defaultuser"), "shared") {
            @Override
            protected List<String> getOwnerRoles(final BoxFile.Info info) {
                return List.of("owneruser");
            }

            @Override
            protected List<String> toRoles(final Collection<BoxCollaboration.Info> collaborations) {
                return List.of("fileuser", "shared");
            }
        };
        final BoxFile.Info info =
                info("{\"has_collaborations\":true," + "\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"100\"}]},"
                        + "\"shared_link\":{\"effective_access\":\"company\"}}");

        final List<String> roles = resolver.getRoles(info, List.of("baseuser"));

        assertEquals(List.of("baseuser", "defaultuser", "owneruser", "folderuser", "shared", "fileuser"), roles);
    }
}

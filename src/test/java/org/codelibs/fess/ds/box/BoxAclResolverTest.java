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
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BoxAclResolverTest {

    /** Builds a {@link BoxFile.Info} from a raw JSON payload without any network access. */
    private static BoxFile.Info info(final String json) {
        return new BoxFile(null, "file-1").new Info(json);
    }

    /**
     * Builds a {@link BoxFolder.Info} from a raw JSON payload without any network access.
     *
     * <p>{@code getID()} on the returned info reflects {@code id}, the underlying
     * {@link BoxFolder}'s own id - not any {@code "id"} field inside {@code json}. Box's SDK
     * ties an {@code Info}'s id to its resource, not to the JSON body used to populate it; only
     * <em>nested</em> objects (e.g. {@code path_collection} entries), which the SDK parses by
     * constructing a fresh resource per entry, pick their id up from JSON.</p>
     */
    private static BoxFolder.Info folderInfo(final String id, final String json) {
        return new BoxFolder(null, id).new Info(json);
    }

    @Test
    public void test_isEffectiveCollaboration_acceptsOnlyReadableAccepted() {
        assertTrue(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.VIEWER));
        assertTrue(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.EDITOR));
        assertTrue(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.CO_OWNER));
        assertTrue(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.PREVIEWER));

        // Uploader cannot preview or download, so it must not grant search access.
        assertFalse(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, BoxCollaboration.Role.UPLOADER));

        // Pending and rejected collaborators have no access yet.
        assertFalse(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.PENDING, BoxCollaboration.Role.EDITOR));
        assertFalse(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.REJECTED, BoxCollaboration.Role.EDITOR));

        // Missing values must not grant access.
        assertFalse(BoxAclResolver.isEffectiveCollaboration(null, BoxCollaboration.Role.EDITOR));
        assertFalse(BoxAclResolver.isEffectiveCollaboration(BoxCollaboration.Status.ACCEPTED, null));
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
        /** The client most recently handed to {@link #loadFolderRoles}, so tests can pin R17's contract. */
        BoxClient lastClient;
        /** When true, {@link #getFileCollaborations} throws instead of returning an empty list. */
        boolean failFileCollaborations;
        /**
         * When true, the 3-arg {@link #loadFolderRoles(BoxClient, String, BoxDataStore.DocumentQuality)}
         * override falls through to the real (unstubbed) implementation instead of delegating to the
         * 2-arg testing stub, so its own try/catch - the thing round 3 added - actually runs.
         */
        boolean failFolderRoles;

        StubResolver(final Map<String, List<String>> folderRoles, final List<String> defaultPermissions,
                final String companySharedLinkRole) {
            super(defaultPermissions, companySharedLinkRole);
            this.folderRoles = folderRoles;
        }

        @Override
        protected List<String> loadFolderRoles(final BoxClient client, final String folderId) {
            folderLookups++;
            lastClient = client;
            return folderRoles.getOrDefault(folderId, List.of());
        }

        @Override
        protected List<String> loadFolderRoles(final BoxClient client, final String folderId, final BoxDataStore.DocumentQuality quality) {
            if (failFolderRoles) {
                return super.loadFolderRoles(client, folderId, quality);
            }
            return loadFolderRoles(client, folderId);
        }

        @Override
        protected List<String> getOwnerRoles(final BoxItem.Info info) {
            return ownerRoles;
        }

        @Override
        protected Collection<BoxCollaboration.Info> getFileCollaborations(final BoxFile.Info info) {
            fileCollaborationLookups++;
            if (failFileCollaborations) {
                throw new RuntimeException("simulated file collaboration lookup failure");
            }
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

        assertEquals(List.of("1user"), resolver.getFolderRoles(null, "100"));
        assertEquals(List.of("1user"), resolver.getFolderRoles(null, "100"));
        assertEquals(List.of("1user"), resolver.getFolderRoles(null, "100"));

        assertEquals(1, resolver.folderLookups, "the folder must only be read once");
    }

    @Test
    public void test_getFolderRoles_retriesAfterFailure() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null) {
            private boolean failedOnce;

            @Override
            protected List<String> loadFolderRoles(final BoxClient client, final String folderId) {
                folderLookups++;
                if (!failedOnce) {
                    failedOnce = true;
                    return null;
                }
                return List.of("1user");
            }
        };

        assertEquals(List.of(), resolver.getFolderRoles(null, "100"), "a failed lookup must not poison the result");
        assertEquals(List.of("1user"), resolver.getFolderRoles(null, "100"), "the retry must return the real roles");
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
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        assertEquals(List.of(), resolver.getOwnerRoles(info("{}")));
    }

    @Test
    public void test_getRoles_hasCollaborationsFalse_skipsFileLookup() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        resolver.getRoles(null, info("{\"has_collaborations\":false}"), List.of());

        assertEquals(0, resolver.fileCollaborationLookups, "the optimisation must skip the per-file lookup");
    }

    @Test
    public void test_getRoles_hasCollaborationsTrue_performsFileLookup() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        resolver.getRoles(null, info("{\"has_collaborations\":true}"), List.of());

        assertEquals(1, resolver.fileCollaborationLookups);
    }

    @Test
    public void test_getRoles_fileCollaborationLookupFailure_setsAclDegraded() {
        // Every pre-existing getRoles test above goes through the 3-arg null-quality overload;
        // this is the first to drive the real 4-arg overload with a live DocumentQuality.
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);
        resolver.failFileCollaborations = true;
        final BoxFile.Info info = info("{\"has_collaborations\":true}");
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        final List<String> roles = resolver.getRoles(null, info, List.of(), quality);

        assertTrue(quality.aclDegraded, "a failed file collaboration lookup must degrade the document");
        assertEquals(List.of(), roles, "the file falls back to owner and ancestor roles alone");
    }

    @Test
    public void test_getRoles_ancestorFolderCollaborationLookupFailure_setsAclDegraded() {
        // Unlike the other tests in this file, this one must NOT let StubResolver's loadFolderRoles
        // override swallow the failure: failFolderRoles routes through the real, unstubbed
        // loadFolderRoles(client, folderId, quality), whose own try/catch is what round 3 added.
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);
        resolver.failFolderRoles = true;
        final BoxClient client = new BoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                throw new RuntimeException("simulated folder collaboration lookup failure");
            }
        };
        final BoxFile.Info info =
                info("{\"has_collaborations\":false,\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"100\"}]}}");
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        final List<String> roles = resolver.getRoles(client, info, List.of(), quality);

        assertTrue(quality.aclDegraded, "a failed ancestor folder lookup must degrade the document");
        assertEquals(List.of(), roles, "the failed folder contributes no roles");
    }

    @Test
    public void test_getRoles_nullPathCollectionAndOwner_doesNotThrow() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);

        assertDoesNotThrow(() -> resolver.getRoles(null, info("{\"has_collaborations\":false}"), List.of()));
    }

    @Test
    public void test_getRoles_skipsRootFolderAncestor() {
        final StubResolver resolver = new StubResolver(Map.of("0", List.of("wronguser"), "100", List.of("gooduser")), List.of(), null);
        final BoxFile.Info info = info(
                "{\"has_collaborations\":false,\"path_collection\":{\"total_count\":2,\"entries\":[{\"id\":\"0\"},{\"id\":\"100\"}]}}");

        final List<String> roles = resolver.getRoles(null, info, List.of());

        assertEquals(List.of("gooduser"), roles);
        assertEquals(1, resolver.folderLookups, "the root folder id \"0\" must never be looked up");
    }

    @Test
    public void test_getRoles_passesCallTimeClientToFolderLookup() {
        // R17: the resolver holds no client of its own - each call must reach the folder
        // lookup with whichever client the caller passed in that time, since each user now
        // crawls through its own BoxClient. new BoxClient() performs no network access, so a
        // plain instance is a safe, cheap sentinel to prove identity with assertSame.
        final StubResolver resolver = new StubResolver(Map.of("100", List.of("gooduser")), List.of(), null);
        final BoxClient callTimeClient = new BoxClient();
        final BoxFile.Info info =
                info("{\"has_collaborations\":false,\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"100\"}]}}");

        resolver.getRoles(callTimeClient, info, List.of());

        assertSame(callTimeClient, resolver.lastClient, "the client passed to getRoles must reach the folder lookup unchanged");
    }

    @Test
    public void test_getRoles_sharedLinkCompanyAccess_addsConfiguredRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), "shared-role");
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"company\"}}");

        assertTrue(resolver.getRoles(null, info, List.of()).contains("shared-role"));
    }

    @Test
    public void test_getRoles_sharedLinkNonCompanyAccess_doesNotAddRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), "shared-role");
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"open\"}}");

        assertFalse(resolver.getRoles(null, info, List.of()).contains("shared-role"));
    }

    @Test
    public void test_getRoles_companySharedLinkRoleBlank_neverAddsRole() {
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);
        final BoxFile.Info info = info("{\"has_collaborations\":false,\"shared_link\":{\"effective_access\":\"company\"}}");

        assertEquals(List.of(), resolver.getRoles(null, info, List.of()));
    }

    @Test
    public void test_getRoles_mergesAllSourcesPreservingOrderAndDedupingAcrossThem() {
        final StubResolver resolver = new StubResolver(Map.of("100", List.of("folderuser", "shared")), List.of("defaultuser"), "shared") {
            @Override
            protected List<String> getOwnerRoles(final BoxItem.Info info) {
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

        final List<String> roles = resolver.getRoles(null, info, List.of("baseuser"));

        assertEquals(List.of("baseuser", "defaultuser", "owneruser", "folderuser", "shared", "fileuser"), roles);
    }

    // --- getRoles(BoxClient, BoxFolder.Info, ...): a folder indexed as a document itself ---

    @Test
    public void test_getRoles_folder_ownCollaborationsIncluded() {
        final StubResolver resolver = new StubResolver(Map.of("200", List.of("owncollaborator")), List.of(), null);
        final BoxFolder.Info info = folderInfo("200", "{\"path_collection\":{\"total_count\":0,\"entries\":[]}}");

        final List<String> roles = resolver.getRoles(null, info, List.of());

        assertEquals(List.of("owncollaborator"), roles);
    }

    @Test
    public void test_getRoles_folder_skipsRootFolderAncestor() {
        final StubResolver resolver =
                new StubResolver(Map.of("0", List.of("wronguser"), "100", List.of("gooduser"), "200", List.of()), List.of(), null);
        final BoxFolder.Info info =
                folderInfo("200", "{\"path_collection\":{\"total_count\":2,\"entries\":[{\"id\":\"0\"},{\"id\":\"100\"}]}}");

        final List<String> roles = resolver.getRoles(null, info, List.of());

        assertEquals(List.of("gooduser"), roles);
        // "0" (ancestor) must never be looked up; "100" (ancestor) and "200" (the folder's own
        // roles) are the only two real lookups.
        assertEquals(2, resolver.folderLookups);
    }

    @Test
    public void test_getRoles_folder_ownCollaborationLookupFailure_setsAclDegraded() {
        // Like test_getRoles_ancestorFolderCollaborationLookupFailure_setsAclDegraded, this must
        // route through the real, unstubbed loadFolderRoles(client, folderId, quality) so its own
        // try/catch actually runs for the folder's *own* lookup, not just its ancestors'.
        final StubResolver resolver = new StubResolver(Map.of(), List.of(), null);
        resolver.failFolderRoles = true;
        final BoxClient client = new BoxClient() {
            @Override
            public Collection<BoxCollaboration.Info> getFolderCollaborations(final String folderId) {
                throw new RuntimeException("simulated folder collaboration lookup failure");
            }
        };
        final BoxFolder.Info info = folderInfo("200", "{\"path_collection\":{\"total_count\":0,\"entries\":[]}}");
        final BoxDataStore.DocumentQuality quality = new BoxDataStore.DocumentQuality();

        final List<String> roles = resolver.getRoles(client, info, List.of(), quality);

        assertTrue(quality.aclDegraded, "a failed own-folder collaboration lookup must degrade the document");
        assertEquals(List.of(), roles, "the failed folder contributes no roles");
    }

    @Test
    public void test_getRoles_folder_mergesAllSourcesPreservingOrderAndDedupingAcrossThem() {
        final StubResolver resolver =
                new StubResolver(Map.of("100", List.of("folderuser", "shared"), "200", List.of("owncollaborator", "shared")),
                        List.of("defaultuser"), "shared") {
                    @Override
                    protected List<String> getOwnerRoles(final BoxItem.Info info) {
                        return List.of("owneruser");
                    }
                };
        final BoxFolder.Info info = folderInfo("200", "{\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"100\"}]},"
                + "\"shared_link\":{\"effective_access\":\"company\"}}");

        final List<String> roles = resolver.getRoles(null, info, List.of("baseuser"));

        assertEquals(List.of("baseuser", "defaultuser", "owneruser", "folderuser", "shared", "owncollaborator"), roles);
    }

    @Test
    public void test_getRoles_folder_sharesFolderRoleCacheWithDescendantFileAncestorLookup() {
        // The brief's central claim: indexing a folder costs at most one lookup that a
        // descendant file's ancestor-role resolution would have made anyway. Prove the cache is
        // the same one, regardless of which of the two call sites (folder-as-document, or
        // file-as-ancestor) reaches folder "100" first.
        final StubResolver resolver = new StubResolver(Map.of("100", List.of("shareduser")), List.of(), null);
        final BoxFolder.Info folder = folderInfo("100", "{\"path_collection\":{\"total_count\":0,\"entries\":[]}}");
        final BoxFile.Info file =
                info("{\"has_collaborations\":false,\"path_collection\":{\"total_count\":1,\"entries\":[{\"id\":\"100\"}]}}");

        final List<String> folderRoles = resolver.getRoles(null, folder, List.of());
        final List<String> fileRoles = resolver.getRoles(null, file, List.of());

        assertTrue(folderRoles.contains("shareduser"));
        assertTrue(fileRoles.contains("shareduser"));
        assertEquals(1, resolver.folderLookups, "the second call must hit the cache, not read folder 100 again");
    }
}

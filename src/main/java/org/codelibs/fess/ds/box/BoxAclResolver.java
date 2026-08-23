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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.ds.box.BoxDataStore.DocumentQuality;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxSharedLink;
import com.box.sdk.BoxUser;

/**
 * Resolves the effective search roles of a Box file.
 *
 * <p>Box does not document whether {@code /files/{id}/collaborations} includes
 * collaborations inherited from parent folders. This resolver therefore always
 * composes the file's own collaborations with those of every ancestor folder
 * found in {@code path_collection}: if the API already returns inherited
 * entries they merely duplicate, and if it does not, nothing is lost.</p>
 *
 * <p>Ancestor folders are resolved through a cache, so the per-file cost drops
 * from one API call per file to one call per folder plus one call for each file
 * that actually carries its own collaborations.</p>
 */
public class BoxAclResolver {

    private static final Logger logger = LogManager.getLogger(BoxAclResolver.class);

    /**
     * The identifier Box uses for the current user's own "All Files" root.
     *
     * <p>Unlike every other folder id, {@code "0"} is not globally unique: it
     * identifies a different physical folder for each impersonated user, yet
     * every file's {@code path_collection} starts with it. Caching roles under
     * this key would leak the first crawled user's root collaborations onto
     * every other user's files, so it is always skipped as an ancestor.</p>
     */
    protected static final String ROOT_FOLDER_ID = "0";

    /** Cached folder identifier to search roles. */
    protected final Map<String, List<String>> folderRoleCache = new ConcurrentHashMap<>();

    /** Roles applied to every document, already encoded. */
    protected final List<String> defaultPermissions;

    /** Role granted to files whose shared link is open to the whole enterprise, or null. */
    protected final String companySharedLinkRole;

    /**
     * Constructs a resolver.
     *
     * <p>This resolver no longer holds a client: each user now crawls through its own
     * {@link BoxClient} instance, so the client that can read a folder's collaborations
     * is only known at call time and is passed into {@link #getRoles(BoxClient, BoxFile.Info, List)}
     * instead.</p>
     *
     * <p><b>Cache assumption.</b> The folder role cache is keyed by folder id alone and is
     * populated by whichever user's walker reaches a folder first, which depends on the order
     * Box enumerates users in. Folder ids being globally unique (the sole exception,
     * {@value #ROOT_FOLDER_ID}, is always skipped as an ancestor) makes the <em>key</em> safe,
     * but that is not the property at risk: what this assumes is that a folder's collaboration
     * list is <em>identical for every caller who can read it</em>. Box's
     * {@code /folders/{id}/collaborations} may not honour that - {@code can_non_owners_view_collaborators}
     * and non-owner access can both narrow what a given caller sees - and a truncated read still
     * succeeds, so {@code aclDegraded} is never set, nothing releases the dedup claim and nothing
     * retries. The result would be silent, order-dependent under-permissioning. This is
     * unverified: it needs a live tenant to settle (design section 13, items 2 and 3), and the
     * caching strategy is deliberately left alone until then.</p>
     *
     * @param defaultPermissions encoded roles applied to every document
     * @param companySharedLinkRole role for enterprise-wide shared links, or null to disable
     */
    public BoxAclResolver(final List<String> defaultPermissions, final String companySharedLinkRole) {
        this.defaultPermissions = defaultPermissions;
        this.companySharedLinkRole = companySharedLinkRole;
    }

    /**
     * Returns whether a per-file collaboration lookup is required.
     *
     * <p>The flag is only populated when {@code has_collaborations} was requested
     * through {@code fields}. A null flag therefore means "unknown", and this
     * returns true so that a missing hint never silently drops a file's access
     * control list.</p>
     *
     * @param hasCollaborations the value reported by Box, or null if not requested
     * @return true if the file may carry its own collaborations
     */
    static boolean hasCollaborations(final Boolean hasCollaborations) {
        return hasCollaborations == null || hasCollaborations.booleanValue();
    }

    /**
     * Returns the search roles of a folder, reading it at most once.
     *
     * <p>A failed lookup is not cached: {@link Map#computeIfAbsent} stores no
     * mapping when the mapping function returns null, so the next file that
     * needs this folder retries instead of being permanently stuck with an
     * empty result.</p>
     *
     * @param client the client to read the folder's collaborations with
     * @param folderId the folder identifier
     * @return the folder's search roles
     */
    protected List<String> getFolderRoles(final BoxClient client, final String folderId) {
        return getFolderRoles(client, folderId, null);
    }

    protected List<String> getFolderRoles(final BoxClient client, final String folderId, final DocumentQuality quality) {
        final List<String> roles = folderRoleCache.computeIfAbsent(folderId, id -> loadFolderRoles(client, id, quality));
        return roles != null ? roles : List.of();
    }

    /**
     * Reads a folder's collaborations from Box.
     *
     * @param client the client to read the folder's collaborations with
     * @param folderId the folder identifier
     * @return the folder's search roles, or null if they could not be read
     */
    protected List<String> loadFolderRoles(final BoxClient client, final String folderId) {
        return loadFolderRoles(client, folderId, null);
    }

    /**
     * Reads a folder's collaborations from Box.
     *
     * @param client the client to read the folder's collaborations with
     * @param folderId the folder identifier
     * @param quality optional degradation tracker; if collaboration lookup fails, {@code quality.aclDegraded} is set to true
     * @return the folder's search roles, or null if they could not be read
     */
    protected List<String> loadFolderRoles(final BoxClient client, final String folderId, final DocumentQuality quality) {
        try {
            return toRoles(client.getFolderCollaborations(folderId));
        } catch (final Exception e) {
            logger.warn("Failed to read collaborations of folder {}. Will retry the next time a file needs it.", folderId, e);
            if (quality != null) {
                quality.aclDegraded = true;
            }
            return null;
        }
    }

    /**
     * Returns whether a collaboration grants read access that should be
     * reflected in the search index.
     *
     * <p>Only accepted collaborations grant access at all; pending and rejected
     * collaborators cannot open the file. The uploader role can neither preview
     * nor download, so it must not grant search access either.</p>
     *
     * @param status the collaboration status
     * @param role the collaboration role
     * @return true if the collaboration should contribute a search role
     */
    static boolean isEffectiveCollaboration(final BoxCollaboration.Status status, final BoxCollaboration.Role role) {
        return status == BoxCollaboration.Status.ACCEPTED && role != null && role != BoxCollaboration.Role.UPLOADER;
    }

    /**
     * Converts collaborations into Fess search roles, keeping only those that
     * grant read access.
     *
     * <p>The single conversion in this plugin: {@code file.roles} reaches it through
     * {@link #getRoles(BoxClient, BoxFile.Info, List)}, and the older script-level
     * {@code file.api.collaborationRoles} delegates to it too.</p>
     *
     * @param collaborations the collaborations
     * @return the search roles
     */
    protected List<String> toRoles(final Collection<BoxCollaboration.Info> collaborations) {
        if (collaborations == null) {
            return List.of();
        }
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final List<String> roles = new ArrayList<>();
        for (final BoxCollaboration.Info c : collaborations) {
            if (!isEffectiveCollaboration(c.getStatus(), c.getRole())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("skipping collaboration: status={}, role={}", c.getStatus(), c.getRole());
                }
                continue;
            }
            final BoxCollaborator.Info accessibleBy = c.getAccessibleBy();
            if (accessibleBy == null) {
                continue;
            }
            switch (accessibleBy.getType()) {
            case USER:
                roles.add(systemHelper.getSearchRoleByUser(accessibleBy.getID()));
                if (StringUtil.isNotBlank(accessibleBy.getLogin())) {
                    roles.add(systemHelper.getSearchRoleByUser(accessibleBy.getLogin()));
                }
                break;
            case GROUP:
                roles.add(systemHelper.getSearchRoleByGroup(accessibleBy.getID()));
                if (StringUtil.isNotBlank(accessibleBy.getLogin())) {
                    roles.add(systemHelper.getSearchRoleByGroup(accessibleBy.getLogin()));
                }
                break;
            default:
                if (logger.isDebugEnabled()) {
                    logger.debug("unknown accessibleBy type: {}", accessibleBy.getType());
                }
                break;
            }
        }
        return roles;
    }

    /**
     * Merges role lists, dropping blanks and duplicates while preserving order.
     *
     * @param sources the role lists to merge; null entries are ignored
     * @return the merged roles
     */
    @SafeVarargs
    static List<String> merge(final Collection<String>... sources) {
        final Set<String> merged = new LinkedHashSet<>();
        for (final Collection<String> source : sources) {
            if (source == null) {
                continue;
            }
            for (final String value : source) {
                if (StringUtil.isNotBlank(value)) {
                    merged.add(value.trim());
                }
            }
        }
        return new ArrayList<>(merged);
    }

    /**
     * Resolves the effective search roles of a file.
     *
     * @param client the client to read ancestor folder collaborations with; it must be
     *        scoped to the same user the file's own collaborations are read as
     * @param info the file information; {@code path_collection}, {@code owned_by},
     *        {@code has_collaborations} and {@code shared_link} must have been requested
     * @param baseRoles roles supplied by the caller, typically the data store
     *        configuration's permissions
     * @return the effective search roles
     */
    public List<String> getRoles(final BoxClient client, final BoxFile.Info info, final List<String> baseRoles) {
        return getRoles(client, info, baseRoles, null);
    }

    /**
     * Resolves the search roles granted to a file.
     *
     * @param client the Box client
     * @param info the file information
     * @param baseRoles default roles to include
     * @param quality optional degradation tracker; if collaboration lookup fails, {@code quality.aclDegraded} is set to true
     * @return the file's resolved search roles
     */
    protected List<String> getRoles(final BoxClient client, final BoxFile.Info info, final List<String> baseRoles,
            final DocumentQuality quality) {
        final List<String> ownerRoles = getOwnerRoles(info);
        final List<String> ancestorRoles = getAncestorRoles(client, info, quality);

        List<String> fileRoles = List.of();
        if (hasCollaborations(info.getHasCollaborations())) {
            try {
                fileRoles = toRoles(getFileCollaborations(info));
            } catch (final Exception e) {
                logger.warn("Failed to read collaborations of file {}. Falling back to ancestor folders and the owner.", info.getID(), e);
                if (quality != null) {
                    quality.aclDegraded = true;
                }
            }
        }

        return merge(baseRoles, defaultPermissions, ownerRoles, ancestorRoles, fileRoles, getSharedLinkRoles(info));
    }

    /**
     * Resolves the effective search roles of a folder, for use when the folder is itself
     * indexed as a document (see {@code ignore_folder}).
     *
     * @param client the client to read ancestor and own folder collaborations with; it must be
     *        scoped to the same identity the folder was walked as
     * @param info the folder information; {@code path_collection}, {@code owned_by} and
     *        {@code shared_link} must have been requested
     * @param baseRoles roles supplied by the caller, typically the data store configuration's permissions
     * @return the effective search roles
     */
    public List<String> getRoles(final BoxClient client, final BoxFolder.Info info, final List<String> baseRoles) {
        return getRoles(client, info, baseRoles, null);
    }

    /**
     * Resolves the search roles granted to a folder.
     *
     * <p>Unlike a file, a folder's own access is not read through a dedicated "does this file
     * carry its own collaborations" lookup: it is read through
     * {@link #getFolderRoles(BoxClient, String, DocumentQuality)} - the very cache a descendant
     * file's ancestor-role lookup already populates - so indexing the folder itself costs at most
     * one lookup that a descendant file would have made anyway.</p>
     *
     * @param client the Box client
     * @param info the folder information
     * @param baseRoles default roles to include
     * @param quality optional degradation tracker; if a collaboration lookup fails, {@code quality.aclDegraded} is set to true
     * @return the folder's resolved search roles
     */
    protected List<String> getRoles(final BoxClient client, final BoxFolder.Info info, final List<String> baseRoles,
            final DocumentQuality quality) {
        final List<String> ownerRoles = getOwnerRoles(info);
        final List<String> ancestorRoles = getAncestorRoles(client, info, quality);
        final List<String> ownRoles = getFolderRoles(client, info.getID(), quality);

        return merge(baseRoles, defaultPermissions, ownerRoles, ancestorRoles, ownRoles, getSharedLinkRoles(info));
    }

    /**
     * Resolves the search roles contributed by every ancestor folder in an item's
     * {@code path_collection}, skipping the user-specific root ({@value #ROOT_FOLDER_ID}).
     *
     * @param client the client to read ancestor folder collaborations with
     * @param info the item information
     * @param quality optional degradation tracker; if a collaboration lookup fails, {@code quality.aclDegraded} is set to true
     * @return the merged ancestor search roles
     */
    private List<String> getAncestorRoles(final BoxClient client, final BoxItem.Info info, final DocumentQuality quality) {
        final List<String> ancestorRoles = new ArrayList<>();
        final List<BoxFolder.Info> pathCollection = info.getPathCollection();
        if (pathCollection != null) {
            for (final BoxFolder.Info ancestor : pathCollection) {
                if (!ROOT_FOLDER_ID.equals(ancestor.getID())) {
                    ancestorRoles.addAll(getFolderRoles(client, ancestor.getID(), quality));
                }
            }
        }
        return ancestorRoles;
    }

    /**
     * Resolves the search role granted by an item's enterprise-wide shared link, if any.
     *
     * @param info the item information
     * @return a single-element list with {@link #companySharedLinkRole}, or an empty list
     */
    private List<String> getSharedLinkRoles(final BoxItem.Info info) {
        final List<String> sharedLinkRoles = new ArrayList<>();
        if (StringUtil.isNotBlank(companySharedLinkRole)) {
            final BoxSharedLink sharedLink = info.getSharedLink();
            if (sharedLink != null && BoxSharedLink.Access.COMPANY == sharedLink.getEffectiveAccess()) {
                sharedLinkRoles.add(companySharedLinkRole);
            }
        }
        return sharedLinkRoles;
    }

    /**
     * Resolves the search roles granted to an item's owner.
     *
     * @param info the item information
     * @return the owner's search roles, or an empty list if there is no owner
     */
    protected List<String> getOwnerRoles(final BoxItem.Info info) {
        final List<String> ownerRoles = new ArrayList<>();
        final BoxUser.Info owner = info.getOwnedBy();
        if (owner != null) {
            final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
            ownerRoles.add(systemHelper.getSearchRoleByUser(owner.getID()));
            if (StringUtil.isNotBlank(owner.getLogin())) {
                ownerRoles.add(systemHelper.getSearchRoleByUser(owner.getLogin()));
            }
        }
        return ownerRoles;
    }

    /**
     * Reads a file's own collaborations from Box.
     *
     * @param info the file information
     * @return the file's collaborations
     */
    protected Collection<BoxCollaboration.Info> getFileCollaborations(final BoxFile.Info info) {
        final Iterable<BoxCollaboration.Info> iterable = info.getResource().getAllFileCollaborations();
        if (iterable == null) {
            return List.of();
        }
        final List<BoxCollaboration.Info> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }
}

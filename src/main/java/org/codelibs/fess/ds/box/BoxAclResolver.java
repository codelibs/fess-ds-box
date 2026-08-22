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
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxSharedLink;
import com.box.sdk.BoxUser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Cached folder identifier to search roles. */
    protected final Map<String, List<String>> folderRoleCache = new ConcurrentHashMap<>();

    /** The client used to read collaborations. */
    protected final BoxClient client;

    /** Roles applied to every document, already encoded. */
    protected final List<String> defaultPermissions;

    /** Role granted to files whose shared link is open to the whole enterprise, or null. */
    protected final String companySharedLinkRole;

    /**
     * Constructs a resolver.
     *
     * @param client the Box client
     * @param defaultPermissions encoded roles applied to every document
     * @param companySharedLinkRole role for enterprise-wide shared links, or null to disable
     */
    public BoxAclResolver(final BoxClient client, final List<String> defaultPermissions, final String companySharedLinkRole) {
        this.client = client;
        this.defaultPermissions = defaultPermissions;
        this.companySharedLinkRole = companySharedLinkRole;
    }

    /**
     * Returns whether a per-file collaboration lookup is required.
     *
     * <p>The {@code has_collaborations} field is only present when it was asked
     * for through {@code fields}. When it is absent or the payload cannot be
     * parsed this returns true, so that a missing hint never silently drops a
     * file's access control list.</p>
     *
     * @param json the raw item JSON
     * @return true if the file may carry its own collaborations
     */
    static boolean hasCollaborations(final String json) {
        if (StringUtil.isBlank(json)) {
            return true;
        }
        try {
            final JsonNode node = MAPPER.readTree(json).get("has_collaborations");
            if (node == null || !node.isBoolean()) {
                return true;
            }
            return node.asBoolean();
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to read has_collaborations from {}", json, e);
            }
            return true;
        }
    }

    /**
     * Returns the search roles of a folder, reading it at most once.
     *
     * @param folderId the folder identifier
     * @return the folder's search roles
     */
    protected List<String> getFolderRoles(final String folderId) {
        return folderRoleCache.computeIfAbsent(folderId, this::loadFolderRoles);
    }

    /**
     * Reads a folder's collaborations from Box.
     *
     * @param folderId the folder identifier
     * @return the folder's search roles, or an empty list if they cannot be read
     */
    protected List<String> loadFolderRoles(final String folderId) {
        try {
            return toRoles(client.getFolderCollaborations(folderId));
        } catch (final Exception e) {
            logger.warn("Failed to read collaborations of folder {}. Its documents will only inherit "
                    + "the roles resolved from other sources.", folderId, e);
            return List.of();
        }
    }

    /**
     * Converts collaborations into Fess search roles, keeping only those that
     * grant read access.
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
            if (!BoxDataStore.BoxFileAPI.isEffectiveCollaboration(c.getStatus(), c.getRole())) {
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
     * @param info the file information; {@code path_collection}, {@code owned_by},
     *        {@code has_collaborations} and {@code shared_link} must have been requested
     * @param baseRoles roles supplied by the caller, typically the data store
     *        configuration's permissions
     * @return the effective search roles
     */
    public List<String> getRoles(final BoxFile.Info info, final List<String> baseRoles) {
        final List<String> ownerRoles = new ArrayList<>();
        final BoxUser.Info owner = info.getOwnedBy();
        if (owner != null) {
            final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
            ownerRoles.add(systemHelper.getSearchRoleByUser(owner.getID()));
            if (StringUtil.isNotBlank(owner.getLogin())) {
                ownerRoles.add(systemHelper.getSearchRoleByUser(owner.getLogin()));
            }
        }

        final List<String> ancestorRoles = new ArrayList<>();
        final List<BoxFolder.Info> pathCollection = info.getPathCollection();
        if (pathCollection != null) {
            for (final BoxFolder.Info ancestor : pathCollection) {
                ancestorRoles.addAll(getFolderRoles(ancestor.getID()));
            }
        }

        List<String> fileRoles = List.of();
        if (hasCollaborations(info.getJson())) {
            try {
                fileRoles = toRoles(getFileCollaborations(info));
            } catch (final Exception e) {
                logger.warn("Failed to read collaborations of file {}. Falling back to ancestor folders " + "and the owner.", info.getID(),
                        e);
            }
        }

        final List<String> sharedLinkRoles = new ArrayList<>();
        if (StringUtil.isNotBlank(companySharedLinkRole)) {
            final BoxSharedLink sharedLink = info.getSharedLink();
            if (sharedLink != null && BoxSharedLink.Access.COMPANY == sharedLink.getEffectiveAccess()) {
                sharedLinkRoles.add(companySharedLinkRole);
            }
        }

        return merge(baseRoles, defaultPermissions, ownerRoles, ancestorRoles, fileRoles, sharedLinkRoles);
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

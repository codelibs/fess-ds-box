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

import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxFile;

/**
 * Drives {@link BoxAclResolver#toRoles} directly.
 *
 * <p>Separate from {@link BoxAclResolverTest} because this is the one part of the resolver that
 * needs a container: it resolves role names through {@code ComponentUtil.getSystemHelper()}.
 * {@link BoxAclResolverTest}'s {@code StubResolver} overrides {@code toRoles} in every one of its
 * cases, so without this class the USER/GROUP dispatch, the dual id-and-login emission and the
 * {@code accessibleBy == null} skip would never execute in any test - and a wrong branch there
 * either leaks a document to someone who cannot open it in Box, or makes it unfindable by
 * everyone who can.</p>
 */
public class BoxAclResolverRolesTest extends UnitDsTestCase {

    @Override
    public String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    public boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // convention.xml does not auto-provide systemHelper in this narrow test classpath. A bare
        // instance is enough: getSearchRoleByUser/getSearchRoleByGroup only read FessConfig, which
        // is auto-provided, so no @PostConstruct init() is needed.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
    }

    /** Builds a {@link BoxCollaboration.Info} from a raw JSON payload without any network access. */
    private static BoxCollaboration.Info collaboration(final String json) {
        return new BoxCollaboration(null, "collab-1").new Info(json);
    }

    private static BoxCollaboration.Info acceptedUser() {
        return collaboration("{\"type\":\"collaboration\",\"id\":\"c1\",\"status\":\"accepted\",\"role\":\"editor\","
                + "\"accessible_by\":{\"type\":\"user\",\"id\":\"u1\",\"login\":\"u1@example.com\",\"name\":\"User One\"}}");
    }

    private static BoxCollaboration.Info acceptedGroup() {
        return collaboration("{\"type\":\"collaboration\",\"id\":\"c2\",\"status\":\"accepted\",\"role\":\"viewer\","
                + "\"accessible_by\":{\"type\":\"group\",\"id\":\"g1\",\"login\":\"engineering\",\"name\":\"Engineering\"}}");
    }

    private static BoxCollaboration.Info acceptedWithoutAccessibleBy() {
        return collaboration("{\"type\":\"collaboration\",\"id\":\"c3\",\"status\":\"accepted\",\"role\":\"editor\"}");
    }

    private static BoxCollaboration.Info pendingUser() {
        return collaboration("{\"type\":\"collaboration\",\"id\":\"c4\",\"status\":\"pending\",\"role\":\"editor\","
                + "\"accessible_by\":{\"type\":\"user\",\"id\":\"pending-user\",\"login\":\"pending@example.com\"}}");
    }

    private static BoxCollaboration.Info uploaderUser() {
        return collaboration("{\"type\":\"collaboration\",\"id\":\"c5\",\"status\":\"accepted\",\"role\":\"uploader\","
                + "\"accessible_by\":{\"type\":\"user\",\"id\":\"uploader-user\",\"login\":\"uploader@example.com\"}}");
    }

    @Test
    public void test_toRoles_userAndGroupPrefixesDiffer() {
        // Everything below rests on this: if the user and group prefixes were the same, swapping
        // the USER and GROUP branches would go unnoticed.
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();

        org.junit.jupiter.api.Assertions.assertNotEquals(systemHelper.getSearchRoleByUser("same-name"),
                systemHelper.getSearchRoleByGroup("same-name"), "a user role and a group role of the same name must differ");
    }

    @Test
    public void test_toRoles_emitsIdAndLoginPerCollaboratorTypeAndSkipsTheRest() {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        final List<String> roles =
                resolver.toRoles(List.of(acceptedUser(), acceptedGroup(), acceptedWithoutAccessibleBy(), pendingUser(), uploaderUser()));

        assertEquals("a user contributes its id and its login as user roles, a group as group roles, in that order",
                List.of(systemHelper.getSearchRoleByUser("u1"), systemHelper.getSearchRoleByUser("u1@example.com"),
                        systemHelper.getSearchRoleByGroup("g1"), systemHelper.getSearchRoleByGroup("engineering")),
                roles);
    }

    @Test
    public void test_toRoles_acceptedUserIsResolvedAsAUserNotAGroup() {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        final List<String> roles = resolver.toRoles(List.of(acceptedUser()));

        assertTrue("the collaborator's id must become a user role", roles.contains(systemHelper.getSearchRoleByUser("u1")));
        assertFalse("a user must never be emitted as a group role", roles.contains(systemHelper.getSearchRoleByGroup("u1")));
    }

    @Test
    public void test_toRoles_acceptedGroupIsResolvedAsAGroupNotAUser() {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        final List<String> roles = resolver.toRoles(List.of(acceptedGroup()));

        assertTrue("the collaborator's id must become a group role", roles.contains(systemHelper.getSearchRoleByGroup("g1")));
        assertFalse("a group must never be emitted as a user role", roles.contains(systemHelper.getSearchRoleByUser("g1")));
    }

    @Test
    public void test_toRoles_collaborationWithoutAccessibleBy_isSkippedWithoutThrowing() {
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        assertEquals("a collaboration with no accessible_by grants nothing", List.of(),
                resolver.toRoles(List.of(acceptedWithoutAccessibleBy())));
    }

    @Test
    public void test_toRoles_pendingAndUploaderGrantNothing() {
        // The security-relevant half of the filter: a pending collaborator cannot open the file
        // yet, and an uploader can neither preview nor download it.
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        assertEquals("a pending collaboration must grant nothing", List.of(), resolver.toRoles(List.of(pendingUser())));
        assertEquals("an uploader collaboration must grant nothing", List.of(), resolver.toRoles(List.of(uploaderUser())));
    }

    @Test
    public void test_toRoles_nullAndEmpty() {
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);

        assertEquals(List.of(), resolver.toRoles(null));
        assertEquals(List.of(), resolver.toRoles(List.of()));
    }

    @Test
    public void test_toRoles_userWithoutLogin_emitsOnlyTheId() {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final BoxAclResolver resolver = new BoxAclResolver(List.of(), null);
        final BoxCollaboration.Info info =
                collaboration("{\"type\":\"collaboration\",\"id\":\"c6\",\"status\":\"accepted\",\"role\":\"editor\","
                        + "\"accessible_by\":{\"type\":\"user\",\"id\":\"u9\"}}");

        assertEquals(List.of(systemHelper.getSearchRoleByUser("u9")), resolver.toRoles(List.of(info)));
    }

    // --- file.api.collaborationRoles: the backward-compatible script-level form ---

    @Test
    public void test_getCollaborationRoles_delegatesToToRoles() {
        // Design section 6.3 keeps file.api.collaborationRoles for backward compatibility but
        // requires it to go through BoxAclResolver, so role naming and the status/role filter
        // cannot drift between the two.
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final BoxDataStore.BoxFileAPI api = new BoxDataStore.BoxFileAPI(new BoxFile(null, "file-1")) {
            @Override
            public List<BoxCollaboration.Info> getAllFileCollaborations() {
                return List.of(acceptedUser(), acceptedGroup(), pendingUser(), uploaderUser());
            }
        };

        assertEquals("file.api.collaborationRoles must produce exactly what BoxAclResolver.toRoles produces",
                new BoxAclResolver(List.of(), null).toRoles(List.of(acceptedUser(), acceptedGroup(), pendingUser(), uploaderUser())),
                api.getCollaborationRoles());
        assertEquals("and that means the pending and uploader collaborators are filtered out here too",
                List.of(systemHelper.getSearchRoleByUser("u1"), systemHelper.getSearchRoleByUser("u1@example.com"),
                        systemHelper.getSearchRoleByGroup("g1"), systemHelper.getSearchRoleByGroup("engineering")),
                api.getCollaborationRoles());
    }
}

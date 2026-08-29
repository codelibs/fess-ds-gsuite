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
package org.codelibs.fess.ds.gsuite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.drive.model.User;

/**
 * Tests for {@link DrivePermissionResolver}.
 */
public class DrivePermissionResolverTest extends UnitDsTestCase {

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        ComponentUtil.setFessConfig(new FessConfig.SimpleImpl() {
            private static final long serialVersionUID = 1L;

            @Override
            public String getRoleSearchUserPrefix() {
                return "1";
            }

            @Override
            public String getRoleSearchGroupPrefix() {
                return "2";
            }

            @Override
            public String getRoleSearchRolePrefix() {
                return "R";
            }

            @Override
            public String getRoleSearchDeniedPrefix() {
                return "D";
            }

            @Override
            public boolean isLdapIgnoreNetbiosName() {
                return false;
            }
        });
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        ComponentUtil.register(new PermissionHelper() {
            {
                systemHelper = new SystemHelper();
            }
        }, "permissionHelper");
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Builds a resolver whose client is never touched, for pure conversion tests.
     * @param params The data store parameters.
     * @return The resolver.
     */
    private DrivePermissionResolver newResolver(final DataStoreParams params) {
        return new DrivePermissionResolver(null, params);
    }

    @Test
    public void test_toRole_user() {
        final Permission permission = new Permission().setType("user").setEmailAddress("a@example.com");
        assertEquals("1a@example.com", newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_group() {
        final Permission permission = new Permission().setType("group").setEmailAddress("g@example.com");
        assertEquals("2g@example.com", newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_domain() {
        final Permission permission = new Permission().setType("domain").setDomain("example.com");
        assertEquals("2example.com", newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_domainWithCustomFormat() {
        final DataStoreParams params = new DataStoreParams();
        params.put("domain_permission_format", "{group}gsuite-{domain}");
        final Permission permission = new Permission().setType("domain").setDomain("example.com");
        assertEquals("2gsuite-example.com", newResolver(params).toRole(permission));
    }

    @Test
    public void test_toRole_domainFormatYieldingNothingUsableIsNull() {
        final DataStoreParams params = new DataStoreParams();
        // No "{domain}" token, so the substituted value is the bare "{group}" prefix with nothing
        // following it. PermissionHelper#encode cannot turn that into a role, so the resolver must
        // drop the permission instead of letting a blank role reach the index. This mirrors
        // GoogleDriveDataStore#getDomainPermission, whose blank guard this resolver replaces.
        params.put("domain_permission_format", "{group}");
        final Permission permission = new Permission().setType("domain").setDomain("example.com");
        assertNull(newResolver(params).toRole(permission));
    }

    @Test
    public void test_toRole_anyone() {
        final Permission permission = new Permission().setType("anyone");
        assertEquals("Rguest", newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_deletedIsExcluded() {
        final Permission permission = new Permission().setType("user").setEmailAddress("a@example.com").setDeleted(Boolean.TRUE);
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_domainWithoutFileDiscoveryIsExcluded() {
        final Permission permission = new Permission().setType("domain").setDomain("example.com").setAllowFileDiscovery(Boolean.FALSE);
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_anyoneWithoutFileDiscoveryIsExcluded() {
        final Permission permission = new Permission().setType("anyone").setAllowFileDiscovery(Boolean.FALSE);
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_userWithoutEmailIsExcluded() {
        final Permission permission = new Permission().setType("user");
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_domainWithoutDomainValueIsExcluded() {
        final Permission permission = new Permission().setType("domain");
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
        // An empty or whitespace-only domain must be dropped too, not substituted into the format.
        assertNull(newResolver(new DataStoreParams()).toRole(new Permission().setType("domain").setDomain("")));
        assertNull(newResolver(new DataStoreParams()).toRole(new Permission().setType("domain").setDomain("   ")));
    }

    @Test
    public void test_toRole_unknownTypeIsExcluded() {
        final Permission permission = new Permission().setType("teamDrive").setEmailAddress("a@example.com");
        assertNull(newResolver(new DataStoreParams()).toRole(permission));
    }

    @Test
    public void test_toRole_owner() {
        final User user = new User().setEmailAddress("o@example.com");
        assertEquals("1o@example.com", newResolver(new DataStoreParams()).toRole(user));
    }

    @Test
    public void test_toRole_ownerWithoutEmailIsExcluded() {
        final User user = new User().setDisplayName("Owner");
        assertNull(newResolver(new DataStoreParams()).toRole(user));
    }

    /**
     * A resolver whose permissions.list calls are recorded instead of issued.
     */
    private static final class RecordingResolver extends DrivePermissionResolver {

        /** Every "id:useDomainAdminAccess" pair passed to listPermissions. */
        private final List<String> calls = new ArrayList<>();

        /** The number of listPermissions invocations. */
        private final AtomicInteger count = new AtomicInteger();

        /** The permissions returned for a shared drive ID. */
        private final List<Permission> drivePermissions;

        /** The permissions returned for a file ID. */
        private final List<Permission> filePermissions;

        RecordingResolver(final DataStoreParams params, final List<Permission> drivePermissions, final List<Permission> filePermissions) {
            super(null, params);
            this.drivePermissions = drivePermissions;
            this.filePermissions = filePermissions;
        }

        @Override
        protected List<Permission> listPermissions(final String id, final boolean useDomainAdminAccess) {
            calls.add(id + ":" + useDomainAdminAccess);
            count.incrementAndGet();
            return useDomainAdminAccess ? drivePermissions : filePermissions;
        }
    }

    @Test
    public void test_resolve_usesInlinePermissionsWithoutApiCall() {
        final RecordingResolver resolver = new RecordingResolver(new DataStoreParams(),
                Arrays.asList(new Permission().setType("user").setEmailAddress("drive@example.com")),
                Arrays.asList(new Permission().setType("user").setEmailAddress("file@example.com")));
        final File file = new File().setId("F1")
                .setDriveId("D1")
                .setOwners(Arrays.asList(new User().setEmailAddress("o@example.com")))
                .setPermissions(Arrays.asList(new Permission().setType("user").setEmailAddress("a@example.com")));

        final List<String> roles = resolver.resolve(file);

        assertEquals(0, resolver.count.get());
        assertEquals(2, roles.size());
        assertTrue(roles.contains("1o@example.com"));
        assertTrue(roles.contains("1a@example.com"));
    }

    @Test
    public void test_resolve_cachesSharedDriveAclPerDrive() {
        final RecordingResolver resolver = new RecordingResolver(new DataStoreParams(),
                Arrays.asList(new Permission().setType("group").setEmailAddress("team@example.com")),
                Arrays.asList(new Permission().setType("user").setEmailAddress("file@example.com")));
        final File first = new File().setId("F1").setDriveId("D1");
        final File second = new File().setId("F2").setDriveId("D1");

        final List<String> firstRoles = resolver.resolve(first);
        final List<String> secondRoles = resolver.resolve(second);

        assertEquals(1, resolver.count.get());
        assertEquals("D1:true", resolver.calls.get(0));
        assertEquals(1, firstRoles.size());
        assertEquals("2team@example.com", firstRoles.get(0));
        assertEquals(firstRoles, secondRoles);
    }

    @Test
    public void test_resolve_fetchesAugmentedPermissionsWithoutDomainAdminAccess() {
        final RecordingResolver resolver = new RecordingResolver(new DataStoreParams(),
                Arrays.asList(new Permission().setType("group").setEmailAddress("team@example.com")),
                Arrays.asList(new Permission().setType("user").setEmailAddress("file@example.com")));
        final File file = new File().setId("F1").setDriveId("D1").setHasAugmentedPermissions(Boolean.TRUE);

        final List<String> roles = resolver.resolve(file);

        assertEquals(2, resolver.count.get());
        assertEquals("D1:true", resolver.calls.get(0));
        assertEquals("F1:false", resolver.calls.get(1));
        assertEquals(2, roles.size());
        assertTrue(roles.contains("2team@example.com"));
        assertTrue(roles.contains("1file@example.com"));
    }

    @Test
    public void test_resolve_returnsEmptyWhenNothingIsResolvable() {
        final RecordingResolver resolver = new RecordingResolver(new DataStoreParams(), new ArrayList<>(), new ArrayList<>());
        final File file = new File().setId("F1");

        final List<String> roles = resolver.resolve(file);

        assertEquals(0, resolver.count.get());
        assertNotNull(roles);
        assertTrue(roles.isEmpty());
    }

    @Test
    public void test_resolve_deduplicatesOwnerAndPermission() {
        final RecordingResolver resolver = new RecordingResolver(new DataStoreParams(), new ArrayList<>(), new ArrayList<>());
        final File file = new File().setId("F1")
                .setOwners(Arrays.asList(new User().setEmailAddress("a@example.com")))
                .setPermissions(Arrays.asList(new Permission().setType("user").setEmailAddress("a@example.com")));

        final List<String> roles = resolver.resolve(file);

        assertEquals(1, roles.size());
        assertEquals("1a@example.com", roles.get(0));
    }
}

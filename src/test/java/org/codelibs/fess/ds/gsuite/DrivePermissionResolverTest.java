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

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
}

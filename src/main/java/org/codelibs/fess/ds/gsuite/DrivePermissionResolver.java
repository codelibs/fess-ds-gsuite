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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;

import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.drive.model.User;

/**
 * Converts Google Drive permissions into Fess search roles.
 * <p>
 * Google group membership is never expanded: a group permission becomes a role named after the
 * group, and Fess is expected to resolve the membership on the user side through SSO or LDAP.
 * </p>
 */
public class DrivePermissionResolver {

    private static final Logger logger = LogManager.getLogger(DrivePermissionResolver.class);

    /** Placeholder inside the domain role format that is replaced with the domain name. */
    protected static final String DOMAIN_PLACEHOLDER = "{domain}";

    /** Role name given to a {@code type=anyone} permission. */
    protected static final String GUEST_ROLE = "guest";

    /** Permission type granted to a single user. */
    protected static final String TYPE_USER = "user";

    /** Permission type granted to a Google group. */
    protected static final String TYPE_GROUP = "group";

    /** Permission type granted to every account in a Google Workspace domain. */
    protected static final String TYPE_DOMAIN = "domain";

    /** Permission type granted to anyone on the internet. */
    protected static final String TYPE_ANYONE = "anyone";

    /** The client used to call permissions.list. */
    protected final GSuiteClient client;

    /** The role format applied to a {@code type=domain} permission. */
    protected final String domainPermissionFormat;

    /** Roles of a shared drive, keyed by drive ID. permissions.list runs once per drive. */
    protected final Map<String, List<String>> driveRoleCache = new ConcurrentHashMap<>();

    /**
     * Constructs a resolver bound to a single client.
     *
     * @param client The client used to call permissions.list. May be null when only the conversion
     *            methods are used.
     * @param params The data store parameters.
     */
    public DrivePermissionResolver(final GSuiteClient client, final DataStoreParams params) {
        this.client = client;
        domainPermissionFormat =
                params.getAsString(GoogleDriveDataStore.DOMAIN_PERMISSION_FORMAT, GoogleDriveDataStore.DEFAULT_DOMAIN_PERMISSION_FORMAT);
    }

    /**
     * Resolves every Fess search role that must be attached to the given file.
     * <p>
     * Three tiers, in order, so that the number of extra API calls stays proportional to the number
     * of shared drives rather than to the number of files:
     * </p>
     * <ol>
     * <li>the inline {@code permissions} of files.list, costing nothing extra;</li>
     * <li>for an item of a shared drive whose inline permissions are absent (the Drive API does not
     * populate them there), the ACL of the shared drive itself, fetched once per drive with
     * {@code useDomainAdminAccess=true} and cached;</li>
     * <li>for an item whose {@code hasAugmentedPermissions} is true, its own permissions, fetched
     * without {@code useDomainAdminAccess} because that flag is only honoured for shared drive
     * IDs.</li>
     * </ol>
     *
     * @param file The file.
     * @return The distinct search roles. Never null, but possibly empty.
     */
    public List<String> resolve(final File file) {
        final Set<String> roles = new LinkedHashSet<>();
        if (file.getOwners() != null) {
            for (final User owner : file.getOwners()) {
                addRole(roles, toRole(owner));
            }
        }
        final List<Permission> inlinePermissions = file.getPermissions();
        if (inlinePermissions != null && !inlinePermissions.isEmpty()) {
            addPermissionRoles(roles, inlinePermissions);
        } else {
            final String driveId = file.getDriveId();
            if (StringUtil.isNotBlank(driveId)) {
                roles.addAll(getDriveRoles(driveId));
            }
        }
        if (Boolean.TRUE.equals(file.getHasAugmentedPermissions()) && StringUtil.isNotBlank(file.getId())) {
            addPermissionRoles(roles, listPermissions(file.getId(), false));
        }
        return new ArrayList<>(roles);
    }

    /**
     * Returns the roles of a shared drive, calling permissions.list at most once per drive ID.
     * <p>
     * A failure is logged and cached as an empty list so that a broken drive does not trigger one
     * failing request per file. The caller then falls back to {@code default_permissions}, and the
     * document is skipped when that is unset.
     * </p>
     *
     * @param driveId The shared drive ID.
     * @return The roles of the shared drive. Never null, but possibly empty.
     */
    protected List<String> getDriveRoles(final String driveId) {
        return driveRoleCache.computeIfAbsent(driveId, id -> {
            final Set<String> roles = new LinkedHashSet<>();
            try {
                addPermissionRoles(roles, listPermissions(id, true));
            } catch (final Exception e) {
                logger.warn("Failed to get the permissions of a shared drive: {}", id, e);
            }
            return new ArrayList<>(roles);
        });
    }

    /**
     * Converts every permission and adds the non-null results to the given set.
     *
     * @param roles The destination set.
     * @param permissions The permissions to convert. May be null.
     */
    protected void addPermissionRoles(final Set<String> roles, final List<Permission> permissions) {
        if (permissions == null) {
            return;
        }
        for (final Permission permission : permissions) {
            addRole(roles, toRole(permission));
        }
    }

    /**
     * Adds a role to the given set unless it is null or blank.
     *
     * @param roles The destination set.
     * @param role The role.
     */
    protected void addRole(final Set<String> roles, final String role) {
        if (StringUtil.isNotBlank(role)) {
            roles.add(role);
        }
    }

    /**
     * Converts a single Drive permission into a Fess search role.
     * <p>
     * Returns null when the permission grants nothing indexable: it was deleted, it is a
     * {@code domain} or {@code anyone} permission with {@code allowFileDiscovery=false} (link-only
     * sharing, which is not discoverable in Drive itself either), it carries no principal value, or
     * its type is not one Fess can express.
     * </p>
     *
     * @param permission The Drive permission.
     * @return The search role, or null when the permission must not become a role.
     */
    public String toRole(final Permission permission) {
        if (permission == null) {
            return null;
        }
        if (Boolean.TRUE.equals(permission.getDeleted())) {
            return null;
        }
        final String type = permission.getType();
        if (type == null) {
            return null;
        }
        switch (type) {
        case TYPE_USER: {
            final String email = permission.getEmailAddress();
            if (StringUtil.isBlank(email)) {
                return null;
            }
            return ComponentUtil.getSystemHelper().getSearchRoleByUser(email);
        }
        case TYPE_GROUP: {
            final String email = permission.getEmailAddress();
            if (StringUtil.isBlank(email)) {
                return null;
            }
            return ComponentUtil.getSystemHelper().getSearchRoleByGroup(email);
        }
        case TYPE_DOMAIN: {
            if (Boolean.FALSE.equals(permission.getAllowFileDiscovery())) {
                return null;
            }
            final String domain = permission.getDomain();
            if (StringUtil.isBlank(domain)) {
                return null;
            }
            final String encoded = ComponentUtil.getPermissionHelper().encode(domainPermissionFormat.replace(DOMAIN_PLACEHOLDER, domain));
            // A format that encodes to nothing usable must drop the permission rather than let a
            // blank role reach the index.
            if (StringUtil.isBlank(encoded)) {
                return null;
            }
            return encoded;
        }
        case TYPE_ANYONE: {
            if (Boolean.FALSE.equals(permission.getAllowFileDiscovery())) {
                return null;
            }
            return ComponentUtil.getSystemHelper().getSearchRoleByRole(GUEST_ROLE);
        }
        default:
            if (logger.isDebugEnabled()) {
                logger.debug("Unsupported permission type: {}", type);
            }
            return null;
        }
    }

    /**
     * Converts a file owner into a Fess search role.
     *
     * @param user The owner.
     * @return The search role, or null when the owner carries no email address.
     */
    public String toRole(final User user) {
        if (user == null) {
            return null;
        }
        final String email = user.getEmailAddress();
        if (StringUtil.isBlank(email)) {
            return null;
        }
        return ComponentUtil.getSystemHelper().getSearchRoleByUser(email);
    }

    /**
     * Calls permissions.list through the bound client. Exists as an override point for tests.
     *
     * @param id The file ID or the shared drive ID.
     * @param useDomainAdminAccess Whether to issue the request as a domain administrator.
     * @return The permissions. Never null.
     */
    protected List<Permission> listPermissions(final String id, final boolean useDomainAdminAccess) {
        return client.getPermissions(id, useDomainAdminAccess);
    }
}

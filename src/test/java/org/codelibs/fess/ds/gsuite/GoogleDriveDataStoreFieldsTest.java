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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

/**
 * Tests for the files.list field projection built by {@link GoogleDriveDataStore}.
 */
public class GoogleDriveDataStoreFieldsTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Splits a projection into the set of field names it mentions, so that a name can be pinned
     * exactly instead of by substring. {@code contains("owners")} would still pass after
     * {@code owners(...)} was dropped and only {@code ownedByMe} remained.
     *
     * @param projection The projection.
     * @return The field names it mentions.
     */
    protected static Set<String> fieldNames(final String projection) {
        return Arrays.stream(projection.split("[^A-Za-z0-9]+")).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }

    /**
     * D-15: the default projection must be an explicit list, never the "*" wildcard.
     */
    @Test
    public void test_buildFileFields_defaultIsExplicit() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String fields = dataStore.buildFileFields(new DataStoreParams());
        assertFalse("the default must not request every field", fields.contains("*"));
        assertTrue("the projection is wrapped", fields.startsWith("nextPageToken,incompleteSearch,files("));
        assertTrue("the projection is closed", fields.endsWith(")"));
        for (final String required : new String[] { "id", "name", "mimeType", "size", "webViewLink", "webContentLink", "trashed",
                "createdTime", "modifiedTime", "driveId", "permissionIds", "hasAugmentedPermissions", "thumbnailLink" }) {
            assertTrue(required + " is requested", fields.contains(required));
        }
        assertTrue("owners are requested with their email", fields.contains("owners(emailAddress"));
        assertTrue("inline permissions are requested with the domain field", fields.contains("permissions(") && fields.contains("domain"));
    }

    /**
     * The projection carries the whole ACL, so narrowing it can silently disable the permission
     * resolution rather than fail loudly.
     * <p>
     * {@link DrivePermissionResolver#resolve} has three tiers and each one reads a field that only
     * the projection can deliver: {@code permissions} feeds the inline ACL of a shared document,
     * {@code driveId} is what sends an item of a shared drive to the cached drive ACL, and
     * {@code hasAugmentedPermissions} is the only signal that an item carries its own ACL on top of
     * the drive's. {@code owners} adds the owner role. Every ACL test builds its {@link
     * com.google.api.services.drive.model.File} objects by hand and never goes through a projection,
     * so dropping one of these names would leave the whole suite green while every crawled document
     * silently lost its roles -- and a document with no role at all is skipped, or worse, indexed
     * without a permission filter.
     * </p>
     */
    @Test
    public void test_defaultFileFields_pinsTheFieldsTheAclResolutionNeeds() {
        final Set<String> names = fieldNames(GoogleDriveDataStore.DEFAULT_FILE_FIELDS);
        for (final String required : new String[] { "permissions", "owners", "driveId", "hasAugmentedPermissions" }) {
            assertTrue(required + " must stay in the default projection or the ACL resolution silently degrades", names.contains(required));
        }
    }

    /**
     * Every field {@code buildFileMap} hands to the script context has to be in the projection.
     * A field that is absent is simply null in a crawl script, with nothing logged.
     */
    @Test
    public void test_defaultFileFields_coversEveryFieldTheCrawlReads() {
        final Set<String> names = fieldNames(GoogleDriveDataStore.DEFAULT_FILE_FIELDS);
        for (final String required : new String[] { "id", "name", "description", "mimeType", "size", "kind", "fileExtension",
                "fullFileExtension", "originalFilename", "md5Checksum", "headRevisionId", "iconLink", "thumbnailLink", "thumbnailVersion",
                "hasThumbnail", "webViewLink", "webContentLink", "exportLinks", "createdTime", "modifiedTime", "modifiedByMe",
                "modifiedByMeTime", "viewedByMe", "viewedByMeTime", "trashed", "explicitlyTrashed", "trashedTime", "trashingUser",
                "parents", "folderColorRgb", "owners", "ownedByMe", "lastModifyingUser", "shared", "driveId", "teamDriveId", "permissions",
                "permissionIds", "hasAugmentedPermissions", "capabilities", "quotaBytesUsed", "version", "writersCanShare",
                "viewersCanCopyContent", "copyRequiresWriterPermission", "isAppAuthorized", "appProperties", "contentHints",
                "imageMediaMetadata", "videoMediaMetadata" }) {
            assertTrue(required + " is read by the crawl and must be requested", names.contains(required));
        }
    }

    /**
     * Breaking change 6: "*" stays available for scripts that reference an unusual field.
     */
    @Test
    public void test_buildFileFields_wildcardIsStillAccepted() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "*");
        assertEquals("*", dataStore.buildFileFields(paramMap));
        paramMap.put("fields", " * ");
        assertEquals("*", dataStore.buildFileFields(paramMap));
    }

    /**
     * A bare field list is wrapped; an already complete projection is passed through verbatim.
     */
    @Test
    public void test_buildFileFields_wrapsBareListAndPassesProjection() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "id,name");
        assertEquals("nextPageToken,incompleteSearch,files(id,name)", dataStore.buildFileFields(paramMap));
        paramMap.put("fields", "nextPageToken,files(id)");
        assertEquals("nextPageToken,files(id)", dataStore.buildFileFields(paramMap));
    }

    /**
     * A blank value is not a projection, so it falls back to the default rather than asking
     * files.list for nothing.
     */
    @Test
    public void test_buildFileFields_blankFallsBackToTheDefault() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("fields", "   ");
        assertEquals(dataStore.buildFileFields(new DataStoreParams()), dataStore.buildFileFields(paramMap));
    }

    /**
     * The inner projection (without the files(...) wrapper) is what changes.list needs.
     */
    @Test
    public void test_getFileFieldProjection() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        assertFalse("the inner projection is not wrapped", dataStore.getFileFieldProjection(paramMap).contains("files("));
        assertTrue(dataStore.getFileFieldProjection(paramMap).contains("id"));
        paramMap.put("fields", "*");
        assertEquals("*", dataStore.getFileFieldProjection(paramMap));
    }
}

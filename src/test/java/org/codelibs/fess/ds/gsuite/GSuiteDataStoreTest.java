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

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.gsuite.UnitDsTestCase;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;

public class GSuiteDataStoreTest extends UnitDsTestCase {

    private GoogleDriveDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new GoogleDriveDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void testGetName() {
        assertEquals("GoogleDriveDataStore", dataStore.getName());
    }

    @Test
    public void testIsIgnoreFolder_True() {
        final DataStoreParams params = new DataStoreParams();
        params.put("ignore_folder", "true");
        assertTrue(dataStore.isIgnoreFolder(params));
    }

    @Test
    public void testIsIgnoreFolder_False() {
        final DataStoreParams params = new DataStoreParams();
        params.put("ignore_folder", "false");
        assertFalse(dataStore.isIgnoreFolder(params));
    }

    @Test
    public void testIsIgnoreFolder_Default() {
        final DataStoreParams params = new DataStoreParams();
        assertTrue(dataStore.isIgnoreFolder(params));
    }

    @Test
    public void testIsIgnoreError_True() {
        final DataStoreParams params = new DataStoreParams();
        params.put("ignore_error", "true");
        assertTrue(dataStore.isIgnoreError(params));
    }

    @Test
    public void testIsIgnoreError_False() {
        final DataStoreParams params = new DataStoreParams();
        params.put("ignore_error", "false");
        assertFalse(dataStore.isIgnoreError(params));
    }

    @Test
    public void testIsIgnoreError_Default() {
        final DataStoreParams params = new DataStoreParams();
        assertTrue(dataStore.isIgnoreError(params));
    }

    @Test
    public void testGetMaxSize_WithValidValue() {
        final DataStoreParams params = new DataStoreParams();
        params.put("max_size", "5000000");
        assertEquals(5000000L, dataStore.getMaxSize(params));
    }

    @Test
    public void testGetMaxSize_WithInvalidValue() {
        final DataStoreParams params = new DataStoreParams();
        params.put("max_size", "invalid");
        assertEquals(10000000L, dataStore.getMaxSize(params));
    }

    @Test
    public void testGetMaxSize_Default() {
        final DataStoreParams params = new DataStoreParams();
        assertEquals(10000000L, dataStore.getMaxSize(params));
    }

    @Test
    public void testGetSupportedMimeTypes_Default() {
        final DataStoreParams params = new DataStoreParams();
        final String[] mimeTypes = dataStore.getSupportedMimeTypes(params);
        assertNotNull(mimeTypes);
        assertEquals(1, mimeTypes.length);
        assertEquals(".*", mimeTypes[0]);
    }

    @Test
    public void testGetSupportedMimeTypes_Custom() {
        final DataStoreParams params = new DataStoreParams();
        params.put("supported_mimetypes", "application/pdf, text/plain, image/.*");
        final String[] mimeTypes = dataStore.getSupportedMimeTypes(params);
        assertNotNull(mimeTypes);
        assertEquals(3, mimeTypes.length);
        assertEquals("application/pdf", mimeTypes[0]);
        assertEquals("text/plain", mimeTypes[1]);
        assertEquals("image/.*", mimeTypes[2]);
    }

    @Test
    public void testNewFixedThreadPool() {
        final ExecutorService executor = dataStore.newFixedThreadPool(4);
        assertNotNull(executor);
        executor.shutdown();
    }

    @Test
    public void testToDate_WithValidDateTime() {
        final long timestamp = System.currentTimeMillis();
        final DateTime dateTime = new DateTime(timestamp);
        final Date result = dataStore.toDate(dateTime);
        assertNotNull(result);
        assertEquals(timestamp, result.getTime());
    }

    @Test
    public void testToDate_WithNull() {
        final Date result = dataStore.toDate(null);
        assertNull(result);
    }

    @Test
    public void testGetPermission_NullValue() {
        final String result = dataStore.getPermission("user", null);
        assertNull(result);
    }

    @Test
    public void testGetPermission_UnknownType() {
        final String result = dataStore.getPermission("unknown", "value");
        assertNull(result);
    }

    @Test
    public void testGetPermission_WithDeletedPermission() {
        final Permission permission = new Permission();
        permission.setType("user");
        permission.setEmailAddress("user@example.com");
        permission.setDeleted(true);
        final String result = dataStore.getPermission(permission);
        assertNull(result);
    }

    @Test
    public void testGetUrl_WithWebContentLink() {
        final DataStoreParams params = new DataStoreParams();
        final File file = new File();
        file.setWebContentLink("https://drive.google.com/file/d/abc123/view");
        file.setId("abc123");
        final String url = dataStore.getUrl(null, params, file);
        assertEquals("https://drive.google.com/file/d/abc123/view", url);
    }

    @Test
    public void testGetUrl_WithoutWebContentLink() {
        final DataStoreParams params = new DataStoreParams();
        final File file = new File();
        file.setId("abc123");
        final String url = dataStore.getUrl(null, params, file);
        assertEquals("https://drive.google.com/uc?id=abc123&export=download", url);
    }

    @Test
    public void testGetUrl_WithoutWebContentLinkAndId() {
        final DataStoreParams params = new DataStoreParams();
        final File file = new File();
        final String url = dataStore.getUrl(null, params, file);
        assertNull(url);
    }

    @Test
    public void testGetFilePermissions_WithNullPermissionsAndOwners() {
        final File file = new File();
        final List<String> permissions = dataStore.getFilePermissions(null, file);
        assertNotNull(permissions);
        assertEquals(0, permissions.size());
    }

    @Test
    public void testSetExtractorName() {
        dataStore.setExtractorName("customExtractor");
        assertEquals("customExtractor", dataStore.extractorName);
    }

    @Test
    public void testDefaultExtractorName() {
        assertEquals("tikaExtractor", dataStore.extractorName);
    }

    @Test
    public void testDefaultMaxCachedContentSize() {
        assertEquals(10000000L, GoogleDriveDataStore.DEFAULT_MAX_SIZE);
    }

    @Test
    public void testAllConstants() {
        assertEquals("max_size", GoogleDriveDataStore.MAX_SIZE);
        assertEquals("ignore_folder", GoogleDriveDataStore.IGNORE_FOLDER);
        assertEquals("ignore_error", GoogleDriveDataStore.IGNORE_ERROR);
        assertEquals("supported_mimetypes", GoogleDriveDataStore.SUPPORTED_MIMETYPES);
        assertEquals("include_pattern", GoogleDriveDataStore.INCLUDE_PATTERN);
        assertEquals("exclude_pattern", GoogleDriveDataStore.EXCLUDE_PATTERN);
        assertEquals("url_filter", GoogleDriveDataStore.URL_FILTER);
        assertEquals("default_permissions", GoogleDriveDataStore.DEFAULT_PERMISSIONS);
        assertEquals("number_of_threads", GoogleDriveDataStore.NUMBER_OF_THREADS);
    }

    @Test
    public void testFileScriptConstants() {
        assertEquals("file", GoogleDriveDataStore.FILE);
        assertEquals("name", GoogleDriveDataStore.FILE_NAME);
        assertEquals("description", GoogleDriveDataStore.FILE_DESCRIPTION);
        assertEquals("contents", GoogleDriveDataStore.FILE_CONTENTS);
        assertEquals("mimetype", GoogleDriveDataStore.FILE_MIMETYPE);
        assertEquals("filetype", GoogleDriveDataStore.FILE_FILETYPE);
        assertEquals("thumbnail_link", GoogleDriveDataStore.FILE_THUMBNAIL_LINK);
        assertEquals("web_view_link", GoogleDriveDataStore.FILE_WEB_VIEW_LINK);
        assertEquals("web_content_link", GoogleDriveDataStore.FILE_WEB_CONTENT_LINK);
        assertEquals("created_time", GoogleDriveDataStore.FILE_CREATED_TIME);
        assertEquals("modified_time", GoogleDriveDataStore.FILE_MODIFIED_TIME);
        assertEquals("size", GoogleDriveDataStore.FILE_SIZE);
        assertEquals("url", GoogleDriveDataStore.FILE_URL);
        assertEquals("roles", GoogleDriveDataStore.FILE_ROLES);
    }

    @Test
    public void testDefaultThreadPoolTimeoutSeconds() {
        assertEquals(60L, GoogleDriveDataStore.DEFAULT_THREAD_POOL_TIMEOUT_SECONDS);
    }

    @Test
    public void testGoogleAppsMimeTypePattern() {
        assertNotNull(GoogleDriveDataStore.GOOGLE_APPS_MIMETYPE_PATTERN);
        assertTrue(GoogleDriveDataStore.GOOGLE_APPS_MIMETYPE_PATTERN.matcher("application/vnd.google-apps.document").matches());
        assertTrue(GoogleDriveDataStore.GOOGLE_APPS_MIMETYPE_PATTERN.matcher("application/vnd.google-apps.spreadsheet").matches());
        assertFalse(GoogleDriveDataStore.GOOGLE_APPS_MIMETYPE_PATTERN.matcher("application/pdf").matches());
    }

    // Note: buildFileMap() tests are omitted as they require integration test environment
    // with ComponentUtil dependencies (FileTypeHelper) which are not available in unit tests.
    // The functionality is covered by integration tests.

    /**
     * Registers a SystemHelper whose search-role prefixes are fixed, so the assertions do not
     * depend on a FessConfig being available in the unit-test container. The prefixes returned
     * here state the real contract explicitly: {@code SystemHelper#getSearchRoleByUser}/
     * {@code getSearchRoleByGroup}/{@code getSearchRoleByRole} return PREFIX-form roles built by
     * {@code SystemHelper#buildSearchRole(type, name)} as {@code type + name} (e.g.
     * {@code "1alice@example.com"}), using the {@code fess_config.properties} defaults
     * {@code role.search.user.prefix=1}, {@code role.search.group.prefix=2},
     * {@code role.search.role.prefix=R}. The {@code {user}}/{@code {group}}/{@code {role}}
     * bracket notation is only the INPUT syntax {@code PermissionHelper#encode()} accepts; it is
     * never what these methods return, so this stub does not use it.
     */
    private void registerStubSystemHelper() {
        ComponentUtil.register(new SystemHelper() {
            @Override
            public String getSearchRoleByUser(final String name) {
                return "1" + name;
            }

            @Override
            public String getSearchRoleByGroup(final String name) {
                return "2" + name;
            }

            @Override
            public String getSearchRoleByRole(final String name) {
                return "R" + name;
            }
        }, "systemHelper");
    }

    @Test
    public void test_getPermission_anyoneIsGuestRole() {
        registerStubSystemHelper();
        assertEquals("Rguest", dataStore.getPermission("anyone", "anyone"));
    }

    @Test
    public void test_getPermission_anyoneWithoutEmailAddress() {
        registerStubSystemHelper();
        // A type=anyone Permission carries no emailAddress, so the null value must not short-circuit.
        assertEquals("Rguest", dataStore.getPermission("anyone", null));
    }

    @Test
    public void test_getPermission_userIsStillUserRole() {
        registerStubSystemHelper();
        assertEquals("1alice@example.com", dataStore.getPermission("user", "alice@example.com"));
    }

    @Test
    public void test_getPermission_groupIsStillGroupRole() {
        registerStubSystemHelper();
        assertEquals("2team@example.com", dataStore.getPermission("group", "team@example.com"));
    }
}

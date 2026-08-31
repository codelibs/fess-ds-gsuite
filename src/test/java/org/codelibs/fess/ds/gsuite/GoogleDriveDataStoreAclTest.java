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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.drive.model.User;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Tests how {@link GoogleDriveDataStore} resolves a Drive ACL into search roles and what it does
 * when nothing can be resolved.
 */
public class GoogleDriveDataStoreAclTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Registers everything the ACL path resolves through {@link ComponentUtil}.
     * <p>
     * Called from each test body rather than from setUp: {@code ComponentUtil} is JVM-wide and no
     * test class may depend on what another one left behind.
     * </p>
     */
    private void registerHelpers() {
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
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");
    }

    /**
     * A client whose credential setup is stubbed out, so that no service account key is needed to
     * obtain a non-null {@link GSuiteClient} to key the resolver cache with.
     */
    private static final class StubGSuiteClient extends GSuiteClient {

        StubGSuiteClient() {
            super(new DataStoreParams(), new MockDriveTransport());
        }

        @Override
        protected GoogleCredentials createCredentials() {
            return null;
        }

        @Override
        protected HttpRequestInitializer createRequestInitializer(final GoogleCredentials googleCredentials) {
            return request -> {
                // no authentication: this client never reaches the network
            };
        }

        @Override
        public List<Permission> getPermissions(final String fileId, final boolean useDomainAdminAccess) {
            throw new AssertionError("permissions.list must not be called when the ACL is inline");
        }
    }

    /**
     * Builds a data store whose resolver always returns the given roles.
     *
     * @param resolvedRoles The roles the resolver returns.
     * @return The data store.
     */
    private GoogleDriveDataStore newDataStoreResolving(final List<String> resolvedRoles) {
        return new GoogleDriveDataStore() {
            @Override
            protected DrivePermissionResolver getPermissionResolver(final Map<String, Object> configMap, final GSuiteClient client,
                    final DataStoreParams paramMap) {
                return new DrivePermissionResolver(null, paramMap) {
                    @Override
                    public List<String> resolve(final File file) {
                        return new ArrayList<>(resolvedRoles);
                    }
                };
            }
        };
    }

    /** A configMap holding exactly the entries processFile reads, with no URL filter. */
    private Map<String, Object> newConfigMap() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(10000000L));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);
        configMap.put(GoogleDriveDataStore.PERMISSION_RESOLVERS, new ConcurrentHashMap<GSuiteClient, DrivePermissionResolver>());
        return configMap;
    }

    /** A plain text file with everything processFile needs. */
    private File newFile() {
        return new File().setId("abc123")
                .setName("doc.txt")
                .setMimeType("text/plain")
                .setWebViewLink("https://drive.google.com/file/d/abc123/view")
                .setSize(Long.valueOf(11L));
    }

    /**
     * A callback that counts what it was asked to store.
     *
     * @param stored The counter.
     * @return The callback.
     */
    private IndexUpdateCallback newCallback(final AtomicInteger stored) {
        return new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                stored.incrementAndGet();
            }

            @Override
            public long getDocumentSize() {
                return stored.get();
            }

            @Override
            public long getExecuteTime() {
                return 0L;
            }

            @Override
            public void commit() {
                // no-op
            }
        };
    }

    @Test
    public void test_getFilePermissions_usesResolvedRoles() {
        registerHelpers();
        final GoogleDriveDataStore dataStore = newDataStoreResolving(Arrays.asList("1a@example.com"));
        final DataStoreParams paramMap = new DataStoreParams();
        // default_permissions is a fallback, not an addition: a resolvable ACL wins outright.
        paramMap.put("default_permissions", "{role}guest");

        final List<String> permissions = dataStore.getFilePermissions(newConfigMap(), paramMap, null, newFile());

        assertEquals(1, permissions.size());
        assertEquals("1a@example.com", permissions.get(0));
    }

    @Test
    public void test_getFilePermissions_fallsBackToDefaultPermissions() {
        registerHelpers();
        final GoogleDriveDataStore dataStore = newDataStoreResolving(new ArrayList<>());
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}guest,{group}everyone");

        final List<String> permissions = dataStore.getFilePermissions(newConfigMap(), paramMap, null, newFile());

        assertEquals(2, permissions.size());
        assertTrue(permissions.contains("Rguest"));
        assertTrue(permissions.contains("2everyone"));
    }

    @Test
    public void test_getFilePermissions_emptyWhenNoDefaultPermissions() {
        registerHelpers();
        final GoogleDriveDataStore dataStore = newDataStoreResolving(new ArrayList<>());

        final List<String> permissions = dataStore.getFilePermissions(newConfigMap(), new DataStoreParams(), null, newFile());

        assertNotNull(permissions);
        assertTrue(permissions.isEmpty());
    }

    @Test
    public void test_getFilePermissions_defaultPermissionEncodingToNothingIsDropped() {
        registerHelpers();
        final GoogleDriveDataStore dataStore = newDataStoreResolving(new ArrayList<>());
        final DataStoreParams paramMap = new DataStoreParams();
        // "{role}" has nothing following it, so PermissionHelper#encode returns null. A blank role
        // must never reach the index; the document has to be skipped instead.
        paramMap.put("default_permissions", "{role}");

        final List<String> permissions = dataStore.getFilePermissions(newConfigMap(), paramMap, null, newFile());

        assertNotNull(permissions);
        assertTrue(permissions.isEmpty());
    }

    @Test
    public void test_processFile_skipsDocumentWithoutAnyRoleBeforeDownloading() {
        registerHelpers();
        final AtomicBoolean downloaded = new AtomicBoolean(false);
        final AtomicInteger stored = new AtomicInteger();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected DrivePermissionResolver getPermissionResolver(final Map<String, Object> configMap, final GSuiteClient client,
                    final DataStoreParams paramMap) {
                return new DrivePermissionResolver(null, paramMap) {
                    @Override
                    public List<String> resolve(final File file) {
                        return new ArrayList<>();
                    }
                };
            }

            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                downloaded.set(true);
                return "hello world";
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        dataStore.processFile(null, newCallback(stored), newConfigMap(), new DataStoreParams(), scriptMap, new HashMap<>(), null,
                newFile());

        assertNull(capturedError.get());
        assertEquals(0, stored.get());
        // A document that is going to be skipped must not cost a download or an extraction.
        assertFalse(downloaded.get());
    }

    @Test
    public void test_processFile_indexesResolvedRolesFromInlineAcl() {
        registerHelpers();
        final AtomicInteger stored = new AtomicInteger();
        final AtomicReference<Map<String, Object>> capturedResultMap = new AtomicReference<>();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                return "hello world";
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                capturedResultMap.set(resultMap);
                return null;
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };
        final File file = newFile().setOwners(Arrays.asList(new User().setEmailAddress("owner@example.com")))
                .setPermissions(Arrays.asList(new Permission().setType("group").setEmailAddress("team@example.com")));
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        try (StubGSuiteClient stubClient = new StubGSuiteClient()) {
            dataStore.processFile(null, newCallback(stored), newConfigMap(), new DataStoreParams(), scriptMap, new HashMap<>(), stubClient,
                    file);
        }

        assertNull(capturedError.get());
        assertEquals(1, stored.get());
        @SuppressWarnings("unchecked")
        final Map<String, Object> fileMap = (Map<String, Object>) capturedResultMap.get().get(GoogleDriveDataStore.FILE);
        @SuppressWarnings("unchecked")
        final List<String> roles = (List<String>) fileMap.get(GoogleDriveDataStore.FILE_ROLES);
        assertEquals(2, roles.size());
        assertTrue(roles.contains("1owner@example.com"));
        assertTrue(roles.contains("2team@example.com"));
    }

    @Test
    public void test_getPermissionResolver_cachesOneResolverPerClient() {
        registerHelpers();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final Map<String, Object> configMap = newConfigMap();

        try (StubGSuiteClient firstClient = new StubGSuiteClient(); StubGSuiteClient secondClient = new StubGSuiteClient()) {
            final DrivePermissionResolver first = dataStore.getPermissionResolver(configMap, firstClient, new DataStoreParams());
            final DrivePermissionResolver again = dataStore.getPermissionResolver(configMap, firstClient, new DataStoreParams());
            final DrivePermissionResolver second = dataStore.getPermissionResolver(configMap, secondClient, new DataStoreParams());

            // The shared drive ACL cache lives in the resolver, so it must survive between files.
            assertSame(first, again);
            // With crawl_target=users each impersonated client has its own view of the ACL.
            assertNotSame(first, second);
        }
    }
}

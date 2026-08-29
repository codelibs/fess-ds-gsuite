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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Test;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.auth.oauth2.ServiceAccountCredentials;

/**
 * API layer tests for {@link GSuiteClient} driven by {@link MockDriveTransport}.
 *
 * <p>
 * Each test queues exactly as many responses as it expects requests. {@link MockDriveTransport}
 * throws when a request arrives with no queued response, so a dry queue also proves the client
 * issued no extra calls.
 * </p>
 */
public class GSuiteClientApiTest extends UnitDsTestCase {

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Builds the minimum parameter set the client constructor requires.
     * @return The parameters.
     */
    static DataStoreParams newParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, GSuiteClientTest.VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "svc@example.iam.gserviceaccount.com");
        return params;
    }

    /**
     * Builds parameters whose retry budget is a few milliseconds, so a retry test never sleeps long.
     * @return The parameters.
     */
    static DataStoreParams newFastRetryParams() {
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.MAX_RETRIES, "2");
        params.put(GSuiteClient.RETRY_INITIAL_INTERVAL_MS, "1");
        params.put(GSuiteClient.MAX_BACKOFF_MS, "4");
        return params;
    }

    /**
     * Builds a Drive service bound to the given transport with a no-op request initializer,
     * so that no OAuth token exchange happens during the test.
     * @param transport The transport.
     * @return The Drive service.
     */
    static Drive newDrive(final HttpTransport transport) {
        return new Drive.Builder(transport, GsonFactory.getDefaultInstance(), request -> {
            // no credentials in tests
        }).setApplicationName("fess-ds-gsuite-test").build();
    }

    /**
     * Builds a client whose Drive service is bound to the given transport.
     * <p>
     * The local is deliberately not named {@code drive}: inside the anonymous subclass body the
     * inherited {@link GSuiteClient#drive} field would shadow it.
     * </p>
     * @param params The parameters.
     * @param mockTransport The transport.
     * @return The client.
     */
    static GSuiteClient newClient(final DataStoreParams params, final MockDriveTransport mockTransport) {
        final Drive mockDrive = newDrive(mockTransport);
        return new GSuiteClient(params, mockTransport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        };
    }

    /**
     * Queues a JSON error response.
     * @param mockTransport The transport.
     * @param statusCode The status code.
     * @param body The JSON body.
     */
    static void queueError(final MockDriveTransport mockTransport, final int statusCode, final String body) {
        mockTransport.queue(statusCode, "application/json; charset=UTF-8", body);
    }

    @Test
    public void test_getPermissions_paginatesAndPassesUseDomainAdminAccess() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"nextPageToken\":\"P2\",\"permissions\":[{\"id\":\"p1\",\"type\":\"user\",\"role\":\"reader\","
                + "\"emailAddress\":\"a@example.com\"}]}");
        transport.queueJson(
                "{\"permissions\":[{\"id\":\"p2\",\"type\":\"group\",\"role\":\"writer\"," + "\"emailAddress\":\"g@example.com\"}]}");
        final Drive mockDrive = newDrive(transport);
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            final List<Permission> permissions = client.getPermissions("D1", true);
            assertEquals(2, permissions.size());
            assertEquals("a@example.com", permissions.get(0).getEmailAddress());
            assertEquals("g@example.com", permissions.get(1).getEmailAddress());
            final List<String> urls = transport.getRequestedUrls();
            assertEquals(2, urls.size());
            assertTrue(urls.get(0), urls.get(0).contains("/drive/v3/files/D1/permissions"));
            assertTrue(urls.get(0), urls.get(0).contains("useDomainAdminAccess=true"));
            assertTrue(urls.get(0), urls.get(0).contains("supportsAllDrives=true"));
            assertTrue(urls.get(0), urls.get(0).contains("pageSize=100"));
            assertTrue(urls.get(1), urls.get(1).contains("pageToken=P2"));
        }
    }

    @Test
    public void test_getPermissions_withoutDomainAdminAccess() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson(
                "{\"permissions\":[{\"id\":\"p1\",\"type\":\"user\",\"role\":\"reader\"," + "\"emailAddress\":\"a@example.com\"}]}");
        final Drive mockDrive = newDrive(transport);
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            final List<Permission> permissions = client.getPermissions("F1", false);
            assertEquals(1, permissions.size());
            final List<String> urls = transport.getRequestedUrls();
            assertEquals(1, urls.size());
            assertTrue(urls.get(0), urls.get(0).contains("/drive/v3/files/F1/permissions"));
            assertTrue(urls.get(0), urls.get(0).contains("useDomainAdminAccess=false"));
        }
    }

    @Test
    public void test_getPermissions_emptyResponseReturnsEmptyList() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{}");
        final Drive mockDrive = newDrive(transport);
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            final List<Permission> permissions = client.getPermissions("F1", false);
            assertNotNull(permissions);
            assertTrue(permissions.isEmpty());
            assertEquals(1, transport.getRequestedUrls().size());
        }
    }

    @Test
    public void test_getDrives_paginatesWithDomainAdminAccess() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"nextPageToken\":\"D2\",\"drives\":[{\"id\":\"drive1\",\"name\":\"Sales\"}]}");
        transport.queueJson("{\"drives\":[{\"id\":\"drive2\",\"name\":\"Engineering\"}]}");
        final Drive mockDrive = newDrive(transport);
        final List<String> driveIds = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            client.getDrives(d -> driveIds.add(d.getId()));
            assertEquals(2, driveIds.size());
            assertEquals("drive1", driveIds.get(0));
            assertEquals("drive2", driveIds.get(1));
            final List<String> urls = transport.getRequestedUrls();
            assertEquals(2, urls.size());
            assertTrue(urls.get(0), urls.get(0).contains("/drive/v3/drives"));
            assertTrue(urls.get(0), urls.get(0).contains("useDomainAdminAccess=true"));
            assertTrue(urls.get(0), urls.get(0).contains("pageSize=100"));
            assertTrue(urls.get(1), urls.get(1).contains("pageToken=D2"));
        }
    }

    @Test
    public void test_getDrives_emptyResponse() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{}");
        final Drive mockDrive = newDrive(transport);
        final List<String> driveIds = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            client.getDrives(d -> driveIds.add(d.getId()));
            assertTrue(driveIds.isEmpty());
            assertEquals(1, transport.getRequestedUrls().size());
        }
    }

    @Test
    public void test_getFilesInDrive_scopesToSingleDrive() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"nextPageToken\":\"F2\",\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        transport.queueJson("{\"files\":[{\"id\":\"f2\",\"name\":\"b.txt\"}]}");
        final Drive mockDrive = newDrive(transport);
        final List<String> fileIds = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            client.getFilesInDrive("drive1", "trashed = false", "nextPageToken,files(id,name)",
                    (final File file) -> fileIds.add(file.getId()));
            assertEquals(2, fileIds.size());
            assertEquals("f1", fileIds.get(0));
            assertEquals("f2", fileIds.get(1));
            final List<String> urls = transport.getRequestedUrls();
            assertEquals(2, urls.size());
            assertTrue(urls.get(0), urls.get(0).contains("corpora=drive"));
            assertTrue(urls.get(0), urls.get(0).contains("driveId=drive1"));
            assertTrue(urls.get(0), urls.get(0).contains("includeItemsFromAllDrives=true"));
            assertTrue(urls.get(0), urls.get(0).contains("supportsAllDrives=true"));
            assertTrue(urls.get(1), urls.get(1).contains("pageToken=F2"));
        }
    }

    @Test
    public void test_getFilesInDrive_emptyResponse() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{}");
        final Drive mockDrive = newDrive(transport);
        final List<String> fileIds = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), transport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }
        }) {
            client.getFilesInDrive("drive1", null, null, (final File file) -> fileIds.add(file.getId()));
            assertTrue(fileIds.isEmpty());
            assertEquals(1, transport.getRequestedUrls().size());
        }
    }

    @Test
    public void test_listUsers_paginatesAdminDirectory() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"U2\",\"users\":[{\"primaryEmail\":\"a@example.com\"}]}");
        mockTransport.queueJson("{\"users\":[{\"primaryEmail\":\"b@example.com\"}]}");
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport) {
            @Override
            protected HttpRequestFactory createAdminRequestFactory() {
                return mockTransport.createRequestFactory(request -> {
                    // no credentials in tests
                });
            }
        }) {
            final List<String> users = client.listUsers("orgUnitPath=/Sales");
            assertEquals(2, users.size());
            assertEquals("a@example.com", users.get(0));
            assertEquals("b@example.com", users.get(1));
            final List<String> urls = mockTransport.getRequestedUrls();
            assertEquals(2, urls.size());
            assertTrue(urls.get(0), urls.get(0).contains("admin/directory/v1/users"));
            assertTrue(urls.get(0), urls.get(0).contains("customer=my_customer"));
            assertTrue(urls.get(0), urls.get(0).contains("maxResults=500"));
            assertTrue(urls.get(0), urls.get(0).contains("query=orgUnitPath"));
            assertTrue(urls.get(0), !urls.get(0).contains("pageToken="));
            assertTrue(urls.get(1), urls.get(1).contains("pageToken=U2"));
        }
    }

    @Test
    public void test_listUsers_withoutQuery() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"users\":[{\"primaryEmail\":\"a@example.com\"}]}");
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport) {
            @Override
            protected HttpRequestFactory createAdminRequestFactory() {
                return mockTransport.createRequestFactory(request -> {
                    // no credentials in tests
                });
            }
        }) {
            final List<String> users = client.listUsers(null);
            assertEquals(1, users.size());
            assertEquals("a@example.com", users.get(0));
            final List<String> urls = mockTransport.getRequestedUrls();
            assertEquals(1, urls.size());
            assertFalse(urls.get(0), urls.get(0).contains("query="));
        }
    }

    @Test
    public void test_listUsers_emptyResponse() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{}");
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport) {
            @Override
            protected HttpRequestFactory createAdminRequestFactory() {
                return mockTransport.createRequestFactory(request -> {
                    // no credentials in tests
                });
            }
        }) {
            final List<String> users = client.listUsers(null);
            assertNotNull(users);
            assertTrue(users.isEmpty());
            assertEquals(1, mockTransport.getRequestedUrls().size());
        }
    }

    /** A user without a primary email address must not become a blank entry in the result. */
    @Test
    public void test_listUsers_skipsUserWithoutPrimaryEmail() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"users\":[{\"id\":\"1\"},{\"primaryEmail\":\"a@example.com\"}]}");
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport) {
            @Override
            protected HttpRequestFactory createAdminRequestFactory() {
                return mockTransport.createRequestFactory(request -> {
                    // no credentials in tests
                });
            }
        }) {
            final List<String> users = client.listUsers(null);
            assertEquals(1, users.size());
            assertEquals("a@example.com", users.get(0));
        }
    }

    /**
     * The clone must authenticate as the passed user and share only the transport. Nothing is
     * queued on the transport, so any request the clone issued would fail the test.
     */
    @Test
    public void test_forUser_sharesTransportAndImpersonatesThePassedUser() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport)) {
            client.setApplicationName("fess-ds-gsuite-test");
            final Drive callerDrive = client.getDrive();
            final GSuiteClient userClient = client.forUser("u1@example.com");
            assertNotSame(client, userClient);
            assertSame(client.httpTransport, userClient.httpTransport);
            assertNotSame(client.params, userClient.params);
            assertEquals("u1@example.com", userClient.params.getAsString(GSuiteClient.IMPERSONATE_USER));
            assertNull(client.params.getAsString(GSuiteClient.IMPERSONATE_USER));
            assertNotSame(client.credentials, userClient.credentials);
            assertEquals("u1@example.com", ((ServiceAccountCredentials) userClient.credentials).getServiceAccountUser());
            assertNull(((ServiceAccountCredentials) client.credentials).getServiceAccountUser());
            assertNotSame(client.requestInitializer, userClient.requestInitializer);
            assertNotSame(callerDrive, userClient.getDrive());
            assertEquals("fess-ds-gsuite-test", userClient.applicationName);
            assertTrue(mockTransport.getRequestedUrls().isEmpty());
        }
    }

    /** The clone keeps every other parameter, and the caller's own impersonation is not disturbed. */
    @Test
    public void test_forUser_overridesImpersonateUserAndKeepsOtherParameters() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.MAX_CACHED_CONTENT_SIZE, "2048");
        params.put(GSuiteClient.IMPERSONATE_USER, "admin@example.com");
        try (GSuiteClient client = new GSuiteClient(params, mockTransport)) {
            final GSuiteClient userClient = client.forUser("u2@example.com");
            assertEquals("2048", userClient.params.getAsString(GSuiteClient.MAX_CACHED_CONTENT_SIZE));
            assertEquals(2048, userClient.maxCachedContentSize);
            assertEquals("u2@example.com", userClient.params.getAsString(GSuiteClient.IMPERSONATE_USER));
            assertEquals("u2@example.com", ((ServiceAccountCredentials) userClient.credentials).getServiceAccountUser());
            assertEquals("admin@example.com", client.params.getAsString(GSuiteClient.IMPERSONATE_USER));
            assertEquals("admin@example.com", ((ServiceAccountCredentials) client.credentials).getServiceAccountUser());
        }
    }

    /** permissions.list must send the configured permission page size. */
    @Test
    public void test_getPermissions_sendsConfiguredPageSize() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson(
                "{\"permissions\":[{\"id\":\"p1\",\"type\":\"user\",\"role\":\"reader\"," + "\"emailAddress\":\"a@example.com\"}]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PERMISSION_PAGE_SIZE, "50");
        try (GSuiteClient client = newClient(params, mockTransport)) {
            assertEquals(1, client.getPermissions("F1", false).size());
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=50"));
    }

    /** permissions.list caps pageSize at 100, so a larger value must be clamped instead of drawing a 400. */
    @Test
    public void test_getPermissions_clampsPageSizeToApiLimit() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"permissions\":[]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PERMISSION_PAGE_SIZE, "1000");
        try (GSuiteClient client = newClient(params, mockTransport)) {
            assertTrue("an empty list is returned, never null", client.getPermissions("F1", false).isEmpty());
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=100"));
        assertFalse(url, url.contains("pageSize=1000"));
    }

    /** drives.list is capped at 100 too and takes the same parameter. */
    @Test
    public void test_getDrives_sendsConfiguredPageSize() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"drives\":[{\"id\":\"drive1\",\"name\":\"Sales\"}]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PERMISSION_PAGE_SIZE, "25");
        final List<String> driveIds = new ArrayList<>();
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getDrives(d -> driveIds.add(d.getId()));
        }
        assertEquals(1, driveIds.size());
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=25"));
    }

    /**
     * A throttled permissions.list page is retried and every page is still returned: an ACL that
     * lost a page would silently narrow the roles of every document of the drive.
     */
    @Test
    public void test_getPermissions_retriesAndStillReturnsEveryPage() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"P2\",\"permissions\":[{\"id\":\"p1\",\"type\":\"user\",\"role\":\"reader\","
                + "\"emailAddress\":\"a@example.com\"}]}");
        queueError(mockTransport, 429, "{\"error\":{\"code\":429,\"message\":\"slow down\"}}");
        mockTransport.queueJson(
                "{\"permissions\":[{\"id\":\"p2\",\"type\":\"group\",\"role\":\"reader\"," + "\"emailAddress\":\"g@example.com\"}]}");
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            final List<Permission> permissions = client.getPermissions("D1", true);
            assertEquals(2, permissions.size());
            assertEquals("a@example.com", permissions.get(0).getEmailAddress());
            assertEquals("g@example.com", permissions.get(1).getEmailAddress());
        }
        assertEquals(3, mockTransport.getRequestedUrls().size());
    }

    /**
     * A permanent permissions.list failure must propagate. A partial ACL is not a usable ACL: the
     * resolver caches it per drive and every document of that drive would silently lose the roles
     * carried by the pages that were never read.
     */
    @Test
    public void test_getPermissions_failsInsteadOfReturningAPartialAcl() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"P2\",\"permissions\":[{\"id\":\"p1\",\"type\":\"user\",\"role\":\"reader\","
                + "\"emailAddress\":\"a@example.com\"}]}");
        queueError(mockTransport, 400, "{\"error\":{\"code\":400,\"message\":\"bad request\"}}");
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> fail("a partial ACL must not be reported as a skippable failure"));
            client.getPermissions("D1", true);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("D1"));
        }
    }

    /**
     * drives.list decides the whole scope of a shared drive crawl, so a permanent failure must
     * propagate. Swallowing it would let a crawl that enumerated nothing report success.
     */
    @Test
    public void test_getDrives_failsInsteadOfCrawlingNothing() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 403, "{\"error\":{\"code\":403,\"errors\":[{\"reason\":\"insufficientPermissions\","
                + "\"message\":\"Insufficient permissions\"}],\"message\":\"Insufficient permissions\"}}");
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> fail("an empty drive enumeration must not be reported as a skippable failure"));
            client.getDrives(d -> fail("no shared drive is expected"));
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertNotNull(e.getMessage());
        }
        assertEquals(1, mockTransport.getRequestedUrls().size());
    }

    /** files.list must be paged and must send the configured page size. */
    @Test
    public void test_getFiles_pagesAndSendsPageSize() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"P2\",\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        mockTransport.queueJson("{\"files\":[{\"id\":\"f2\",\"name\":\"b.txt\"}]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PAGE_SIZE, "123");
        final List<String> ids = new ArrayList<>();
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        }
        assertEquals(2, ids.size());
        assertEquals("f1", ids.get(0));
        assertEquals("f2", ids.get(1));
        final List<String> urls = mockTransport.getRequestedUrls();
        assertEquals(2, urls.size());
        assertTrue(urls.get(0), urls.get(0).contains("pageSize=123"));
        assertTrue(urls.get(1), urls.get(1).contains("pageToken=P2"));
    }

    /** A page size above the files.list limit of 1000 must be clamped, not sent verbatim and rejected with a 400. */
    @Test
    public void test_getFiles_clampsPageSizeToApiLimit() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"files\":[]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PAGE_SIZE, "5000");
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> fail("no file is expected"));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=1000"));
        assertFalse(url, url.contains("pageSize=5000"));
    }

    /**
     * A permanent files.list failure must be handed to the failure handler and must not propagate:
     * one broken user or drive may not abort the whole crawl. The pages already read are kept.
     */
    @Test
    public void test_getFiles_reportsFailureInsteadOfThrowing() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"P2\",\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        queueError(mockTransport, 400, "{\"error\":{\"code\":400,\"message\":\"bad request\"}}");
        final List<String> ids = new ArrayList<>();
        final List<String> failedTargets = new ArrayList<>();
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> failedTargets.add(target));
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        }
        assertEquals(1, ids.size());
        assertEquals("f1", ids.get(0));
        assertEquals(1, failedTargets.size());
        assertTrue(failedTargets.get(0), failedTargets.get(0).contains("files.list"));
        assertTrue(failedTargets.get(0), failedTargets.get(0).contains(GSuiteClient.ALL_DRIVES));
    }

    /** The reported target must name the impersonated user, so a failed My Drive listing is attributable. */
    @Test
    public void test_getFiles_failureTargetNamesTheImpersonatedUser() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 400, "{\"error\":{\"code\":400,\"message\":\"bad request\"}}");
        final DataStoreParams params = newFastRetryParams();
        params.put(GSuiteClient.IMPERSONATE_USER, "u1@example.com");
        final List<String> failedTargets = new ArrayList<>();
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.setFailureHandler((target, e) -> failedTargets.add(target));
            client.getFiles(null, GSuiteClient.USER_CORPORA, null, "*", file -> fail("no file is expected"));
        }
        assertEquals(1, failedTargets.size());
        assertTrue(failedTargets.get(0), failedTargets.get(0).contains("u1@example.com"));
    }

    /** An incompleteSearch result is silently short, so it must be surfaced with the corpora and the query. */
    @Test
    public void test_getFiles_incompleteSearchIsReported() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"incompleteSearch\":true,\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        final Drive mockDrive = newDrive(mockTransport);
        final List<String> incompleteTargets = new ArrayList<>();
        final List<String> ids = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }

            @Override
            protected void onIncompleteSearch(final String target) {
                incompleteTargets.add(target);
            }
        }) {
            client.getFiles("name contains 'report'", GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        }
        assertEquals(1, ids.size());
        assertEquals(1, incompleteTargets.size());
        assertTrue(incompleteTargets.get(0), incompleteTargets.get(0).contains(GSuiteClient.ALL_DRIVES));
        assertTrue(incompleteTargets.get(0), incompleteTargets.get(0).contains("name contains 'report'"));
    }

    /** A throttled page is retried and the listing then completes with nothing reported as a failure. */
    @Test
    public void test_getFiles_retriesThrottledPage() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 429, "{\"error\":{\"code\":429,\"message\":\"slow down\"}}");
        mockTransport.queueJson("{\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        final List<String> ids = new ArrayList<>();
        final List<String> failedTargets = new ArrayList<>();
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> failedTargets.add(target));
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        }
        assertEquals(1, ids.size());
        assertEquals(2, mockTransport.getRequestedUrls().size());
        assertTrue(failedTargets.toString(), failedTargets.isEmpty());
    }

    /** getFilesInDrive must send the configured page size too. */
    @Test
    public void test_getFilesInDrive_sendsPageSize() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"files\":[]}");
        final DataStoreParams params = newParams();
        params.put(GSuiteClient.PAGE_SIZE, "250");
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getFilesInDrive("drive1", null, null, file -> fail("no file is expected"));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=250"));
    }

    /**
     * One inaccessible shared drive must be reported and skipped so the remaining drives are still
     * crawled. A 403 that is not a quota error is permanent, so it must not be retried either.
     */
    @Test
    public void test_getFilesInDrive_reportsFailureInsteadOfThrowing() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 403, "{\"error\":{\"code\":403,\"errors\":[{\"reason\":\"insufficientFilePermissions\","
                + "\"message\":\"The user does not have sufficient permissions.\"}],\"message\":\"Insufficient permissions\"}}");
        final List<String> failedTargets = new ArrayList<>();
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> failedTargets.add(target));
            client.getFilesInDrive("drive1", null, null, file -> fail("no file is expected"));
        }
        assertEquals(1, mockTransport.getRequestedUrls().size());
        assertEquals(1, failedTargets.size());
        assertTrue(failedTargets.get(0), failedTargets.get(0).contains("drive1"));
    }

    /** A per-user client must inherit the failure handler, or a failure in one My Drive goes unrecorded. */
    @Test
    public void test_forUser_inheritsFailureHandler() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        final List<String> failedTargets = new ArrayList<>();
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> failedTargets.add(target));
            final GSuiteClient userClient = client.forUser("u1@example.com");
            userClient.failureHandler.accept("files.list", new IOException("boom"));
        }
        assertEquals(1, failedTargets.size());
        assertEquals("files.list", failedTargets.get(0));
    }

    /** Two per-user clients must not share anything that binds a request to one of them. */
    @Test
    public void test_forUser_clientsDoNotShareRequestState() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = new GSuiteClient(newParams(), mockTransport)) {
            final GSuiteClient first = client.forUser("u1@example.com");
            final GSuiteClient second = client.forUser("u2@example.com");
            assertNotSame(first.params, second.params);
            assertNotSame(first.credentials, second.credentials);
            assertNotSame(first.requestInitializer, second.requestInitializer);
            assertNotSame(first.getDrive(), second.getDrive());
            assertSame(first.httpTransport, second.httpTransport);
            assertEquals("u1@example.com", ((ServiceAccountCredentials) first.credentials).getServiceAccountUser());
            assertEquals("u2@example.com", ((ServiceAccountCredentials) second.credentials).getServiceAccountUser());
        }
    }
}

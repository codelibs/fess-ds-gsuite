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
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
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

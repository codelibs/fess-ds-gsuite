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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.Permission;

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
}

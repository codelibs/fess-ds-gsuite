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

import static org.codelibs.fess.ds.gsuite.GSuiteClientApiTest.newClient;
import static org.codelibs.fess.ds.gsuite.GSuiteClientApiTest.newFastRetryParams;
import static org.codelibs.fess.ds.gsuite.GSuiteClientApiTest.newParams;
import static org.codelibs.fess.ds.gsuite.GSuiteClientApiTest.queueError;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Test;

import com.google.api.services.drive.model.Change;

/**
 * Tests for the Drive change feed.
 *
 * <p>
 * Every assertion on the {@code fields} projection is made on the decoded URL: the value travels
 * through {@code GenericUrl}, which percent-encodes the commas of the projection.
 * </p>
 */
public class GSuiteClientChangesTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Decodes a requested URL so the assertions can be written against the projection as sent.
     *
     * @param url The requested URL.
     * @return The decoded URL.
     */
    protected static String decoded(final String url) {
        return URLDecoder.decode(url, StandardCharsets.UTF_8);
    }

    /**
     * changes.getStartPageToken must ask for the drive scope when a drive id is given.
     */
    @Test
    public void test_getStartPageToken_forDrive() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"kind\":\"drive#startPageToken\",\"startPageToken\":\"9001\"}");
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            assertEquals("9001", client.getStartPageToken("0ABCdef"));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("/drive/v3/changes/startPageToken"));
        assertTrue("the drive scope is requested", url.contains("driveId=0ABCdef"));
        assertTrue("shared drives are supported", url.contains("supportsAllDrives=true"));
    }

    /**
     * A null drive id means the My Drive scope of the impersonated user; no driveId is sent.
     */
    @Test
    public void test_getStartPageToken_forUserScope() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"startPageToken\":\"42\"}");
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            assertEquals("42", client.getStartPageToken(null));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertFalse("no drive scope is requested", url.contains("driveId="));
    }

    /**
     * A start page token that cannot be read must surface: without it the scope has no anchor and a
     * caller that took a null for "no changes" would skip the scope entirely.
     */
    @Test
    public void test_getStartPageToken_failurePropagates() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 404, "{\"error\":{\"code\":404,\"message\":\"not found\"}}");
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.getStartPageToken("0ABCdef");
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("0ABCdef"));
        }
    }

    /**
     * changes.list must page and must return newStartPageToken, not nextPageToken. Persisting the
     * wrong one replays the same deltas on the next run.
     * <p>
     * A removed change carries no file, so its file id is the only handle on the document that has
     * to be deleted from the index; it must reach the consumer.
     * </p>
     */
    @Test
    public void test_getChanges_returnsNewStartPageToken() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson(
                "{\"nextPageToken\":\"C2\",\"changes\":[{\"changeType\":\"file\",\"fileId\":\"f1\",\"file\":{\"id\":\"f1\",\"name\":\"a\"}}]}");
        mockTransport
                .queueJson("{\"newStartPageToken\":\"777\",\"changes\":[{\"changeType\":\"file\",\"fileId\":\"f2\",\"removed\":true}]}");
        final List<Change> changes = new ArrayList<>();
        final String token;
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            token = client.getChanges("100", "0ABCdef", changes::add);
        }
        assertEquals("777", token);
        assertEquals(2, changes.size());
        assertEquals("f1", changes.get(0).getFileId());
        assertEquals(Boolean.TRUE, changes.get(1).getRemoved());
        assertNull("a removed change carries no file", changes.get(1).getFile());
        assertEquals("but it does carry the file id the deletion needs", "f2", changes.get(1).getFileId());
        final List<String> urls = mockTransport.getRequestedUrls();
        assertEquals(2, urls.size());
        assertTrue(urls.get(0), urls.get(0).contains("pageToken=100"));
        assertTrue("the second page uses nextPageToken", urls.get(1).contains("pageToken=C2"));
        assertTrue(urls.get(0), urls.get(0).contains("driveId=0ABCdef"));
    }

    /**
     * includeRemoved is the only signal for deletions and revoked access, and its default is false,
     * so it must always be requested explicitly.
     */
    @Test
    public void test_getChanges_alwaysIncludesRemoved() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"newStartPageToken\":\"5\",\"changes\":[]}");
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            client.getChanges("1", null, change -> fail("no change is expected"));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue("includeRemoved", url.contains("includeRemoved=true"));
        assertTrue("includeCorpusRemovals", url.contains("includeCorpusRemovals=true"));
        assertTrue("includeItemsFromAllDrives", url.contains("includeItemsFromAllDrives=true"));
        assertTrue("supportsAllDrives", url.contains("supportsAllDrives=true"));
        assertFalse("no drive scope is requested", url.contains("driveId="));
    }

    /**
     * The file projection configured by the data store must reach changes.list nested under
     * changes(file(...)), and the change level fields the incremental crawl needs must be there too.
     */
    @Test
    public void test_getChanges_usesConfiguredFileFields() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"newStartPageToken\":\"5\",\"changes\":[]}");
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            client.setFileFields("id,name,trashed");
            client.getChanges("1", null, change -> fail("no change is expected"));
        }
        final String url = decoded(mockTransport.getRequestedUrls().get(0));
        assertTrue(url, url.contains("newStartPageToken"));
        assertTrue(url, url.contains("nextPageToken"));
        assertTrue(url, url.contains("changes(changeType,removed,fileId,driveId,time,file(id,name,trashed))"));
    }

    /**
     * changes.list has its own envelope, so a complete files.list projection cannot be nested under
     * changes(file(...)): the API would reject it with a 400 and every page of the feed would fail.
     * The whole file resource is requested instead, which is never less than what was asked for.
     */
    @Test
    public void test_buildChangeFields_doesNotNestACompleteFilesProjection() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            client.setFileFields("nextPageToken,incompleteSearch,files(id,name)");
            final String fields = client.buildChangeFields();
            assertFalse(fields, fields.contains("files("));
            assertFalse(fields, fields.contains("file(nextPageToken"));
            assertEquals("newStartPageToken,nextPageToken,changes(changeType,removed,fileId,driveId,time,file)", fields);
        }
    }

    /**
     * The wildcard escape hatch is not nestable either, and it is also the default when the data
     * store never configured a projection.
     */
    @Test
    public void test_buildChangeFields_wildcardRequestsTheWholeFile() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            assertEquals("newStartPageToken,nextPageToken,changes(changeType,removed,fileId,driveId,time,file)",
                    client.buildChangeFields());
            client.setFileFields("*");
            assertEquals("newStartPageToken,nextPageToken,changes(changeType,removed,fileId,driveId,time,file)",
                    client.buildChangeFields());
        }
    }

    /** A blank projection must not blank out the one already configured. */
    @Test
    public void test_setFileFields_ignoresBlank() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            client.setFileFields("id,name");
            client.setFileFields("   ");
            client.setFileFields(null);
            assertEquals("newStartPageToken,nextPageToken,changes(changeType,removed,fileId,driveId,time,file(id,name))",
                    client.buildChangeFields());
        }
    }

    /**
     * A throttled page is retried like every other Drive call, so a quota blip does not cost the
     * whole feed.
     */
    @Test
    public void test_getChanges_retriesThrottledPage() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 429, "{\"error\":{\"code\":429,\"message\":\"slow down\"}}");
        mockTransport.queueJson("{\"newStartPageToken\":\"9\",\"changes\":[{\"changeType\":\"file\",\"fileId\":\"f1\"}]}");
        final List<String> fileIds = new ArrayList<>();
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            assertEquals("9", client.getChanges("1", null, change -> fileIds.add(change.getFileId())));
        }
        assertEquals(1, fileIds.size());
        assertEquals(2, mockTransport.getRequestedUrls().size());
    }

    /**
     * A failing changes.list must surface so the caller can discard the token and resynchronize.
     * Reporting it and continuing would lose the changes of that page <em>and</em> advance the token
     * past them, which no later run can recover from.
     */
    @Test
    public void test_getChanges_failurePropagates() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        queueError(mockTransport, 404, "{\"error\":{\"code\":404,\"message\":\"invalid token\"}}");
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.setFailureHandler((target, e) -> fail("a lost page must not be reported as a skippable failure"));
            client.getChanges("stale", null, change -> fail("no change is expected"));
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("changes"));
        }
    }

    /**
     * A failure on a later page must not leave a token behind: the caller keeps the one it stored, so
     * the deltas already handed to the consumer are simply replayed on the next run.
     */
    @Test
    public void test_getChanges_failureOnALaterPageYieldsNoToken() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"nextPageToken\":\"C2\",\"changes\":[{\"changeType\":\"file\",\"fileId\":\"f1\"}]}");
        queueError(mockTransport, 400, "{\"error\":{\"code\":400,\"message\":\"bad request\"}}");
        final List<String> fileIds = new ArrayList<>();
        try (GSuiteClient client = newClient(newFastRetryParams(), mockTransport)) {
            client.getChanges("1", null, change -> fileIds.add(change.getFileId()));
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertNotNull(e.getMessage());
        }
        assertEquals(1, fileIds.size());
        assertEquals(2, mockTransport.getRequestedUrls().size());
    }

    /**
     * Without a stored token there is nothing to resume from, so no request is issued and no token is
     * returned. The caller must fall back to a full crawl rather than read this as "no changes".
     */
    @Test
    public void test_getChanges_blankTokenIssuesNoRequest() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        try (GSuiteClient client = newClient(newParams(), mockTransport)) {
            assertNull(client.getChanges(null, null, change -> fail("no change is expected")));
            assertNull(client.getChanges("  ", null, change -> fail("no change is expected")));
        }
        assertTrue(mockTransport.getRequestedUrls().toString(), mockTransport.getRequestedUrls().isEmpty());
    }

    /** changes.list must send the configured page size, clamped like every other listing. */
    @Test
    public void test_getChanges_sendsConfiguredPageSize() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queueJson("{\"newStartPageToken\":\"5\",\"changes\":[]}");
        final org.codelibs.fess.entity.DataStoreParams params = newParams();
        params.put(GSuiteClient.PAGE_SIZE, "250");
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getChanges("1", null, change -> fail("no change is expected"));
        }
        final String url = mockTransport.getRequestedUrls().get(0);
        assertTrue(url, url.contains("pageSize=250"));
    }
}

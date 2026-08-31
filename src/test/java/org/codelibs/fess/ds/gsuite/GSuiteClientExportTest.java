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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.google.api.services.drive.Drive;

/**
 * Tests for the files.export size guard and for the shared drive flag of the media download.
 */
public class GSuiteClientExportTest extends UnitDsTestCase {

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Builds a client whose Drive service and export size limit are both under the test's control.
     * <p>
     * The locals are deliberately not named {@code drive} or {@code params}: inside the anonymous
     * subclass body the inherited fields of {@link GSuiteClient} would shadow them.
     * </p>
     *
     * @param params The parameters.
     * @param mockTransport The transport.
     * @param limit The export size limit, in bytes.
     * @return The client.
     */
    private static GSuiteClient newExportClient(final DataStoreParams params, final MockDriveTransport mockTransport, final long limit) {
        final Drive mockDrive = GSuiteClientApiTest.newDrive(mockTransport);
        return new GSuiteClient(params, mockTransport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }

            @Override
            protected long getExportSizeLimit() {
                return limit;
            }
        };
    }

    /** D-18: the bounded buffer must refuse to grow past the limit. */
    @Test
    public void test_boundedByteArrayOutputStream_rejectsOverflow() throws Exception {
        try (GSuiteClient.BoundedByteArrayOutputStream out = new GSuiteClient.BoundedByteArrayOutputStream(4L)) {
            out.write("abcd".getBytes(StandardCharsets.UTF_8));
            assertEquals(4, out.size());
            try {
                out.write('e');
                fail("Expected ExportSizeLimitExceededException");
            } catch (final GSuiteClient.ExportSizeLimitExceededException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("4"));
            }
        }
    }

    /** A byte array write that straddles the limit is rejected before anything is buffered. */
    @Test
    public void test_boundedByteArrayOutputStream_rejectsStraddlingWrite() throws Exception {
        try (GSuiteClient.BoundedByteArrayOutputStream out = new GSuiteClient.BoundedByteArrayOutputStream(4L)) {
            out.write("ab".getBytes(StandardCharsets.UTF_8));
            try {
                out.write("cde".getBytes(StandardCharsets.UTF_8));
                fail("Expected ExportSizeLimitExceededException");
            } catch (final GSuiteClient.ExportSizeLimitExceededException e) {
                assertEquals("nothing past the limit is buffered", 2, out.size());
            }
        }
    }

    /** An export under the limit is returned as text, through files.export with alt=media. */
    @Test
    public void test_extractFileText_underLimit() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queue(200, "text/plain; charset=UTF-8", "hello drive");
        try (GSuiteClient client = newExportClient(GSuiteClientApiTest.newParams(), mockTransport, GSuiteClient.EXPORT_SIZE_LIMIT)) {
            assertEquals(10L * 1024 * 1024, client.getExportSizeLimit());
            assertEquals("hello drive", client.extractFileText("fileId1", "text/plain"));
        }
        final List<String> urls = mockTransport.getRequestedUrls();
        assertEquals(1, urls.size());
        assertTrue(urls.get(0), urls.get(0).contains("/drive/v3/files/fileId1/export"));
        assertTrue(urls.get(0), urls.get(0).contains("alt=media"));
    }

    /**
     * D-18: an export past the limit must not be buffered whole, and must not be indexed as an empty
     * document either. An empty document is indistinguishable from a real one with no text, so the
     * file is skipped and reported: {@link MaxLengthExceededException} is a
     * {@code CrawlingAccessException}, so the data store records it in the admin failure list exactly
     * as it already does for a file over {@code max_size}.
     */
    @Test
    public void test_extractFileText_overLimitIsSkippedAndReported() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queue(200, "text/plain; charset=UTF-8", "0123456789abcdef");
        try (GSuiteClient client = newExportClient(GSuiteClientApiTest.newParams(), mockTransport, 8L)) {
            client.extractFileText("fileId1", "text/plain");
            fail("Expected MaxLengthExceededException");
        } catch (final MaxLengthExceededException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("fileId1"));
            assertTrue(e.getMessage(), e.getMessage().contains("8"));
        }
    }

    /** A throttled export is retried, so a rate limited download does not lose the content of a file. */
    @Test
    public void test_extractFileText_retriesThrottledExport() {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        GSuiteClientApiTest.queueError(mockTransport, 429, "{\"error\":{\"code\":429,\"message\":\"slow down\"}}");
        mockTransport.queue(200, "text/plain; charset=UTF-8", "hello drive");
        try (GSuiteClient client =
                newExportClient(GSuiteClientApiTest.newFastRetryParams(), mockTransport, GSuiteClient.EXPORT_SIZE_LIMIT)) {
            assertEquals("hello drive", client.extractFileText("fileId1", "text/plain"));
        }
        assertEquals(2, mockTransport.getRequestedUrls().size());
    }

    /** D-19: the media download must carry supportsAllDrives, or a shared drive item cannot be read. */
    @Test
    public void test_getFileInputStream_setsSupportsAllDrives() throws Exception {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queue(200, "text/plain; charset=UTF-8", "body");
        try (GSuiteClient client = newExportClient(GSuiteClientApiTest.newParams(), mockTransport, GSuiteClient.EXPORT_SIZE_LIMIT);
                InputStream in = client.getFileInputStream("fileId1")) {
            assertEquals("body", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        final List<String> urls = mockTransport.getRequestedUrls();
        assertEquals(1, urls.size());
        assertTrue(urls.get(0), urls.get(0).contains("supportsAllDrives=true"));
        assertTrue(urls.get(0), urls.get(0).contains("alt=media"));
    }

    /** A throttled media download is retried too. */
    @Test
    public void test_getFileInputStream_retriesThrottledDownload() throws Exception {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        GSuiteClientApiTest.queueError(mockTransport, 429, "{\"error\":{\"code\":429,\"message\":\"slow down\"}}");
        mockTransport.queue(200, "text/plain; charset=UTF-8", "body");
        try (GSuiteClient client = newExportClient(GSuiteClientApiTest.newFastRetryParams(), mockTransport, GSuiteClient.EXPORT_SIZE_LIMIT);
                InputStream in = client.getFileInputStream("fileId1")) {
            assertEquals("body", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals(2, mockTransport.getRequestedUrls().size());
    }
}

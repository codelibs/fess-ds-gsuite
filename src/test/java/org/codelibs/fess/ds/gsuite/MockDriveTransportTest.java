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

import org.junit.jupiter.api.Test;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

public class MockDriveTransportTest extends UnitDsTestCase {

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Test
    public void test_filesListPaging() throws Exception {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"files\":[{\"id\":\"F1\",\"name\":\"a.txt\"}],\"nextPageToken\":\"P2\"}");
        transport.queueJson("{\"files\":[{\"id\":\"F2\",\"name\":\"b.txt\"}]}");

        final Drive drive = new Drive.Builder(transport, GsonFactory.getDefaultInstance(), null).setApplicationName("test").build();

        final List<String> ids = new ArrayList<>();
        String pageToken = null;
        do {
            final FileList result = drive.files().list().setPageToken(pageToken).execute();
            for (final File file : result.getFiles()) {
                ids.add(file.getId());
            }
            pageToken = result.getNextPageToken();
        } while (pageToken != null);

        assertEquals(2, ids.size());
        assertEquals("F1", ids.get(0));
        assertEquals("F2", ids.get(1));
        assertEquals(2, transport.getRequestedUrls().size());
        assertTrue(transport.getRequestedUrls().get(1), transport.getRequestedUrls().get(1).contains("pageToken=P2"));
    }
}

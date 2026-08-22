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

import org.junit.jupiter.api.Test;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.drive.Drive;

/**
 * Regression test for codelibs/fess-ds-gsuite#21.
 *
 * <p>
 * The bundled Drive API client must be compatible with the {@code google-api-client} version that
 * Fess itself ships ({@code 2.7.2}). The old {@code v3-rev157-1.25.0} client statically asserts a
 * major version of {@code 1} and therefore dies with {@code ExceptionInInitializerError} the moment
 * {@code Drive.Builder#build()} is called inside Fess.
 * </p>
 */
public class Issue21Test extends UnitDsTestCase {

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Test
    public void test_driveBuilderDoesNotThrow() {
        final Drive drive =
                new Drive.Builder(new MockHttpTransport(), GsonFactory.getDefaultInstance(), null).setApplicationName("issue21").build();
        assertNotNull(drive);
    }
}

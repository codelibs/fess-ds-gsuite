/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
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

import com.google.api.services.drive.Drive;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;

public class GSuiteDataStoreTest extends ContainerTestCase {

    private GSuiteDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new GSuiteDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void testProduction() throws Exception {
        // doDriveServiceTest();
        // doStoreDataTest();
    }

    public void testPrivateKey() {
        final String privateKeyPem =
                "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOnYxwfP57dOGx\\nxwY7f3yB17RewqW4bRmTEg/uetU9yA+TgYqNG7r/tPr3gzAQN39Q+Hu5dH2J6JEm\\n9kGLh3XTFuQUNZlTV0jBV7/l0tsfaemHNbGnt1GIJITnxgVWAtxnZb6O3G9p6W9p\\nv2v4o5Lz1NBMmyBTAQU/onAGArj6W26GMAeM5ZNNbscugV22AKNtWwN3xHLnd3eJ\\nKzi2CbwrL/XfUGFEyGD6vVUIWmAre2zq1OxcpcWGKTn2IPefrlIceERwCJvZXxQ5\\nQyk0adTMJq5Qfb3lF1cgndG2P3F+Mcxoo2+RrtJUwBQSJC6Aq1D26fps4CJzn1YC\\n6WKiG7xFAgMBAAECggEACYeZPmf5edrAfSljl3NwG/IFwvgh2iGIFDE5VGPMeZLE\\nayaGrC76/0fK6ocdvKW+pM6tMDbYAngcV8p0Z/nZvKB56Re2yHIGbEp+kpxY2HhT\\nWdXnaYeqRkf+7EzFGrw7i7ZU5XRz3BP0/FDkqz1qLf5jFCF0ineJ1S9KEPDntL5V\\nGRyj4uAjsG0aD0R6QpYf2+7rslyPkTgruiXPqkpHZ5cwHROMf8YjZskQPUAsNr+Q\\nr3ovH6rEXEpKo4Jps2JxJniD2V3PsHZ9QX5n4a6jWDfZEy59f4w4UVmQZLM/Ma5/\\nVwdug8BrcHS1MySir1j6s3bPXV2Nm8u8PzTr+F/jsQKBgQDzWHUA6V9ekQ2Rwqul\\ncJ91jehZMHg17NQud+/bn2kLiv1u87Td0+asN8ywozL1LBquMucl7nMw/gGFD2ni\\nlZM3c7dZlNkCkm1yPhAniz+KLP7l5fBhZ/dN/44ZGYP/4qwo0KfELRRjnNqo+9Uc\\nL2RLCEimZaH2fXYdX4xzyh+WUQKBgQDZXB8BAWbzEtMekNIN+RUsbZ7c5l5DI0T3\\nGglZJESHj8fXYCvHQEy4OBeI0p/qS4NDISBOVHXzGxTOLV1FScgPwzoKeiig0AW/\\n4O5q0Yo+heaxsgE5ghUO8hw2seSiP1c5+01/om7tbT/rMe6felFJsJfQoul7mutq\\nD2+Mz1DltQKBgQCm2wt3MY3kIN/GB058pQmhqEkeBr8WcqpWpoR/+gEkGgyGbHKi\\n++4aPjSLFYwWUkSFF4ApISQ4/qH6I8R9ygPkrOKWeRqHyfFjuSyIgNFzpECvUIgP\\nsiL/h3Bew4EgDsPvRIsUV7i4SNAhuHO63MAPNsHh3qQ8iHBZ2a9Lodcg0QKBgC1C\\nHzqIXjVSwB7nLLW4HY6IrMF2Pj5gg6WoCDZFdPd9GrFf1v3AB7l8BHp60M1qN8Ss\\nixuEPqMGCoj7rSYWPM/7aIRx9y+04N2ZKkuXod9u5iAt3k9pJJVeGD3TQLX/1lu+\\nVd6zpcFONDb2yKbwQyjC2nmY0mDoWwhUenepW0DZAoGAUADLxGsgikcSd25IXWuO\\nUsTWHTJioyUl3r2GXt7hZJU085Mu0N1Ib9X2EslNWTTPPBzV6vwEX6gcBrRWsbRl\\n1hmpqtfGq0uB13/1Xu8W2hmn2dQe65y8Ce/ItOTHDdK9BdACKokpR2xbE33fQ77C\\nxdjr8wdB7GHZHw4yDUOxzb8=\\n-----END PRIVATE KEY-----\\n";
        try {
            final PrivateKey privateKey = GSuiteDataStore.getPrivateKey(privateKeyPem);
        } catch (final Exception e) {
            fail(e.getMessage());
        }
    }

    protected void doDriveServiceTest() throws Exception {
        final String privateKeyPem = "";
        final String privateKeyId = "";
        final String clientEmail = "";
        final PrivateKey privateKey = GSuiteDataStore.getPrivateKey(privateKeyPem);

        try {
            final Drive drive = GSuiteDataStore.getDriveService(privateKey, privateKeyId, clientEmail);
            drive.files().list().execute().getFiles().forEach(f -> {
                System.out.println(f.getName());
            });
        } catch (final IOException e) {
            fail(e.getMessage());
        }
    }

    protected void doStoreDataTest() {
        final DataConfig dataConfig = new DataConfig();
        final IndexUpdateCallback callback = new IndexUpdateCallback() {
            @Override
            public void store(Map<String, String> paramMap, Map<String, Object> dataMap) {
                System.out.println(dataMap);
            }

            @Override
            public long getExecuteTime() {
                return 0;
            }

            @Override
            public long getDocumentSize() {
                return 0;
            }

            @Override
            public void commit() {
            }
        };
        final Map<String, String> paramMap = new HashMap<>();
        paramMap.put("private_key", "");
        paramMap.put("private_key_id", "");
        paramMap.put("client_email", "");
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldTitle(), "files.name");
        scriptMap.put(fessConfig.getIndexFieldContent(), "files.description + \"\\n\" + files.contents");
        scriptMap.put(fessConfig.getIndexFieldMimetype(), "files.mimetype");
        scriptMap.put(fessConfig.getIndexFieldCreated(), "files.created_time");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "files.modified_time");
        scriptMap.put(fessConfig.getIndexFieldUrl(), "files.web_view_link");
        scriptMap.put(fessConfig.getIndexFieldThumbnail(), "files.thumbnail_link");

        dataStore.storeData(dataConfig, callback, paramMap, scriptMap, defaultDataMap);
    }

}

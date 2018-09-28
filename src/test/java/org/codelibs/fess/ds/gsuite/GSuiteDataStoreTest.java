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

    public GSuiteDataStore dataStore;

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
                "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCn+4MuYt7wOw4O\nkm9TSlZioYZOcGMZWb6WC2OYZBUQ3XTsMNLfJtAp6CHPipwwWsl9wMgSR9dGeGiT\nmLnt3h3uGKa6gglzDyzMvJwrACx1VAG7fETV7nkeMYnph/S/smadnM/LIX/8szoX\nYG8IjE+L63tjGaamPP1EXTi9KJzTpf1bMPoK2K8oPsuimWYzEGLuSpoys5T4J8CH\nICIG6WlFrCayiMn3M+XWaWF1SpQsGqwtmXKwx0cGt855jY3RNncuhVUTaV1yav16\nwMjxcncpllAFUEBiPjcDqmTEDXAkjDk47436GWvC3ywgK9f6RJn6NyO6y4umDief\nh9bJcSqrAgMBAAECggEANWj5UTbbhotoUUbLAbMJm2ubHfaK88T+nYt6e7oanpWj\nNvPhIFWhaymlEm2ScqdzGDpDAtj3wC7JpWAygciVQJ5y6Ksu4PgKuQAhMWTpPdbb\nhCajZlfgB7Mdk/SH46mQCf4RbZc1r/22czdUpmWiZy0UurIts/6BUorHXxF6J6rD\nFtssYKbxt9gIwp/SZIIhwyGDDD4enayV3rV/o63nny2CXiIskJoWWFENOlUMQ3ZU\ntTt38dvaVIQaxAbNqbfpgAkP+gxeNCH/V65oucAsFg9SUz1DGVWtih74kX4YOZTl\nPgoCWudLI1g2r3JdcUY9Lu5d/s/cQ7rgjNUbh+hU9QKBgQDoRuTXTstiUykQMR60\ntVUi14IInrhi9FHvPXUjAiHzysfG/7LRrZU5BodzQUQdFEEgv/krSQUQZIhOln23\nfM6KoBvMQyOWX2XLbnW32wD9WkEVEQ2dD+gnRQEAoFmrp3oyQVCS2LG1Chgh0nxk\nRIgpgPsRWZjNIBKfoTRGMPSrbwKBgQC5I5PQFF/6LxkUJ72NEQwOIGl2zgKLOp7A\n0LygtnCYruniIecbE6wRIxAl7Pyitn3pLBrpOfkQ5/+62x/izi97SMPFhwDlSNDz\nl57Xz31HMMuXMwtkUfbWempVUtPazZagkv/UR03gcqv/2LOpQTYtZHWM9Nwket/q\ngKS01OiGhQKBgQDlxlPUxgaeMsPZV64Xd5ZLzEK/QjwG78gp0XMR+acao0uziPkd\nQQjwC4xssHCwD3xJ6H6VmjtfNwQ8MdUVcWpkafR1wSjwNVjewFvFT+mPtvvQ2jjM\nWZL/XdybAQUTsK5KDqClU6WgAOdUdgNFsSG9ZPF4/VgR+xtRPEgY847mEwKBgFjZ\ntip6XMVYrRR7LgDxWXO+Il8d5yKic2Xdx2jppYfhCeW4l4zrF/zxcuKApY4BPtQU\nkqWzaNCwRo6KcvcKNMdZ2HqHm+pt07nT3it8LIdp54scuC7hFzE/dqCUK5AqckuF\nwLlDryA0aE9U1IQ6A6ItQCzxpmTrg2KrcmBGfYFxAoGAfs5ND0bt3DkTpzq0SFej\nibdUv3Ida6rIj+uo78SKP/wjG2eD/mEl0PXT9JQcsEerFh5cdG9UXI182v1YmWSE\nxrwPtlnL//EpDTzRhnDGOy6pReo1jfrYw6PCPiaFbqvN89jgGk7dgdW31TJlOxQP\nAni2P2NjSP8YiS7B6FPYVS8=\n-----END PRIVATE KEY-----\n";
        try {
            final PrivateKey privateKey = GSuiteDataStore.getPrivateKey(privateKeyPem);
        } catch (final Exception e) {
            fail(e.getMessage());
        }
    }

    protected void doDriveServiceTest() throws Exception {
        final String projectId = "";
        final String privateKeyPem = "";
        final String privateKeyId = "";
        final String clientEmail = "";
        final PrivateKey privateKey = GSuiteDataStore.getPrivateKey(privateKeyPem);

        try {
            final Drive drive = GSuiteDataStore.getDriveService(projectId, privateKey, privateKeyId, clientEmail);
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
        paramMap.put("project_id", "");
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

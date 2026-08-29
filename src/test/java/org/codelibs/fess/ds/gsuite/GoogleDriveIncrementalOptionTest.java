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

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for the delete_old_docs suppression required by incremental crawling.
 */
public class GoogleDriveIncrementalOptionTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Builds a data config with the given raw handler parameter.
     *
     * @param handlerParameter The raw handler parameter.
     * @return The data config.
     */
    protected static DataConfig newDataConfig(final String handlerParameter) {
        final DataConfig dataConfig = new DataConfig();
        dataConfig.setHandlerParameter(handlerParameter);
        return dataConfig;
    }

    /**
     * incremental=true must switch off the stale document sweep of DataIndexHelper.
     */
    @Test
    public void test_disableDeleteOldDocs_whenIncremental() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams initParamMap = new DataStoreParams();
        dataStore.disableDeleteOldDocsIfIncremental(newDataConfig("incremental=true"), initParamMap);
        assertEquals("false", initParamMap.getAsString("delete_old_docs"));
    }

    /**
     * A full crawl must leave the sweep alone so the generation replacement model keeps working.
     */
    @Test
    public void test_doesNotTouchDeleteOldDocs_whenFullCrawl() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams absent = new DataStoreParams();
        dataStore.disableDeleteOldDocsIfIncremental(newDataConfig("crawl_target=shared_drives"), absent);
        assertNull(absent.getAsString("delete_old_docs"));

        final DataStoreParams explicitFalse = new DataStoreParams();
        dataStore.disableDeleteOldDocsIfIncremental(newDataConfig("incremental=false"), explicitFalse);
        assertNull(explicitFalse.getAsString("delete_old_docs"));
    }

    /**
     * A null data config must be tolerated.
     */
    @Test
    public void test_nullDataConfig() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams initParamMap = new DataStoreParams();
        dataStore.disableDeleteOldDocsIfIncremental(null, initParamMap);
        assertNull(initParamMap.getAsString("delete_old_docs"));
        assertFalse(dataStore.isIncrementalConfig(null));
    }

    /**
     * An explicit delete_old_docs=true must lose against incremental=true.
     * <p>
     * The two settings together are not a preference but a data loss: an incremental run only sees
     * changed documents, so the sweep would delete every document it did not touch, that is the whole
     * index. The suppression is therefore re-asserted after
     * {@code AbstractDataStore#store} has merged the handler parameters into the very map
     * {@code DataIndexHelper} reads back.
     * </p>
     */
    @Test
    public void test_explicitDeleteOldDocsIsOverridden() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataConfig dataConfig = newDataConfig("incremental=true\ndelete_old_docs=true");
        final DataStoreParams initParamMap = new DataStoreParams();

        // What store() does before delegating.
        dataStore.disableDeleteOldDocsIfIncremental(dataConfig, initParamMap);
        // What AbstractDataStore#store does with the handler parameters.
        initParamMap.putAll(dataConfig.getHandlerParameterMap());
        assertEquals("the merge is what makes the second call necessary", "true", initParamMap.getAsString("delete_old_docs"));
        // What store() does once the delegate returned, successfully or not.
        dataStore.disableDeleteOldDocsIfIncremental(dataConfig, initParamMap);

        assertEquals("false", initParamMap.getAsString("delete_old_docs"));
    }

    /**
     * The incremental flag is read case insensitively, like every other boolean parameter.
     */
    @Test
    public void test_isIncrementalConfig() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertTrue(dataStore.isIncrementalConfig(newDataConfig("incremental=true")));
        assertTrue(dataStore.isIncrementalConfig(newDataConfig("incremental=TRUE")));
        assertFalse(dataStore.isIncrementalConfig(newDataConfig("incremental=yes")));
        assertFalse(dataStore.isIncrementalConfig(newDataConfig("")));
    }
}

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for the start page token persistence.
 */
public class DriveCrawlStateTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * A state whose write back is captured instead of hitting DataConfigBhv.
     */
    protected static class CapturingState extends DriveCrawlState {

        /** The number of times the data config was updated. */
        protected final AtomicInteger updates = new AtomicInteger();

        /**
         * Constructs a new CapturingState.
         *
         * @param dataConfig The data configuration.
         */
        protected CapturingState(final DataConfig dataConfig) {
            super(dataConfig);
        }

        @Override
        protected void updateDataConfig(final DataConfig dataConfig) {
            updates.incrementAndGet();
        }
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
     * The scope keys must be stable and distinguish drives from users.
     */
    @Test
    public void test_scopeKeys() {
        assertEquals("drive:0ABCdef", DriveCrawlState.driveScopeKey("0ABCdef"));
        assertEquals("user:alice@example.com", DriveCrawlState.userScopeKey("alice@example.com"));
    }

    /**
     * A fresh config yields no tokens and no signature.
     */
    @Test
    public void test_emptyState() {
        final DriveCrawlState state = new CapturingState(newDataConfig("crawl_target=shared_drives"));
        assertNull(state.getToken("drive:d1"));
        assertFalse("an absent signature is never compatible", state.isCompatible("sig"));
    }

    /**
     * Tokens stored as a JSON map are read back per scope.
     */
    @Test
    public void test_readsStoredTokens() {
        final DriveCrawlState state = new CapturingState(
                newDataConfig("crawl_target=shared_drives\nstart_page_tokens={\"drive:d1\":\"111\",\"user:alice@example.com\":\"222\"}\n"
                        + "crawl_signature=abc"));
        assertEquals("111", state.getToken("drive:d1"));
        assertEquals("222", state.getToken("user:alice@example.com"));
        assertTrue(state.isCompatible("abc"));
        assertFalse(state.isCompatible("xyz"));
    }

    /**
     * Malformed JSON must degrade to a full crawl rather than abort.
     */
    @Test
    public void test_malformedJsonFallsBackToEmpty() {
        final DriveCrawlState state = new CapturingState(newDataConfig("start_page_tokens=not-json"));
        assertNull(state.getToken("drive:d1"));
    }

    /**
     * The first persist appends both keys and keeps every other line where it was.
     */
    @Test
    public void test_buildHandlerParameter_appendsWhenAbsent() {
        final CapturingState state = new CapturingState(newDataConfig("crawl_target=shared_drives\nimpersonate_user=a@example.com"));
        state.putToken("drive:d1", "111");
        state.setSignature("sig1");
        final String result = state.buildHandlerParameter("crawl_target=shared_drives\nimpersonate_user=a@example.com");
        assertEquals("crawl_target=shared_drives\nimpersonate_user=a@example.com\n"
                + "start_page_tokens={\"drive:d1\":\"111\"}\ncrawl_signature=sig1", result);
    }

    /**
     * A repeat run updates both keys in place and never duplicates them.
     */
    @Test
    public void test_buildHandlerParameter_updatesInPlace() {
        final String stored =
                "crawl_target=shared_drives\nstart_page_tokens={\"drive:d1\":\"111\"}\n" + "crawl_signature=sig1\nnumber_of_threads=4";
        final CapturingState state = new CapturingState(newDataConfig(stored));
        state.putToken("drive:d1", "999");
        state.setSignature("sig2");
        final String result = state.buildHandlerParameter(stored);
        assertEquals(1L, result.lines().filter(l -> l.startsWith("start_page_tokens=")).count());
        assertEquals(1L, result.lines().filter(l -> l.startsWith("crawl_signature=")).count());
        assertTrue(result.lines().anyMatch(l -> l.equals("start_page_tokens={\"drive:d1\":\"999\"}")));
        assertTrue(result.lines().anyMatch(l -> l.equals("crawl_signature=sig2")));
        assertTrue("unrelated lines keep their position", result.lines().anyMatch(l -> l.equals("number_of_threads=4")));
    }

    /**
     * An encrypted value must survive verbatim: rebuilding from getHandlerParameterMap() would write
     * the decrypted private key back into the config index on every crawl.
     */
    @Test
    public void test_buildHandlerParameter_preservesEncryptedValueVerbatim() {
        final String encrypted = "{cipher}" + org.codelibs.fess.util.ComponentUtil.getPrimaryCipher().encrypt("dummy-key-material");
        final String stored = "client_email=a@example.iam.gserviceaccount.com\nprivate_key=" + encrypted + "\n"
                + "start_page_tokens={\"drive:d1\":\"111\"}";
        final CapturingState state = new CapturingState(newDataConfig(stored));
        state.putToken("drive:d1", "222");
        final String result = state.buildHandlerParameter(stored);
        assertTrue(result.lines().anyMatch(l -> l.equals("private_key=" + encrypted)));
        assertEquals(1L, result.lines().filter(l -> l.startsWith("private_key=")).count());
        assertFalse("the decrypted key must never be written back", result.contains("dummy-key-material"));
        assertTrue(result.lines().anyMatch(l -> l.equals("start_page_tokens={\"drive:d1\":\"222\"}")));
    }

    /**
     * Every unrelated line survives verbatim and in order, including a key without a value and a line
     * that carries no '=' at all. Blank lines are dropped, which ParameterUtil.parse ignores anyway.
     */
    @Test
    public void test_buildHandlerParameter_keepsUnrelatedLinesVerbatim() {
        final List<String> unrelated = List.of("crawl_target=both", "number_of_threads=4", "field.script.title=file.name",
                "#not a comment, this is a key", "empty_value=", "max_size=10000000");
        final String stored = "crawl_target=both\nnumber_of_threads=4\n\nfield.script.title=file.name\n"
                + "#not a comment, this is a key\nempty_value=\nstart_page_tokens={\"drive:d1\":\"111\"}\ncrawl_signature=sig1\n"
                + "max_size=10000000";
        final CapturingState state = new CapturingState(newDataConfig(stored));
        state.putToken("drive:d2", "222");
        state.setSignature("sig2");
        final String result = state.buildHandlerParameter(stored);
        final List<String> kept = result.lines()
                .filter(l -> !l.startsWith(DriveCrawlState.START_PAGE_TOKENS + "=") && !l.startsWith(DriveCrawlState.CRAWL_SIGNATURE + "="))
                .collect(Collectors.toList());
        assertEquals("no unrelated line may be dropped, reordered or rewritten", unrelated, kept);
        assertTrue(result.lines().anyMatch(l -> l.equals("start_page_tokens={\"drive:d1\":\"111\",\"drive:d2\":\"222\"}")));
        assertTrue(result.lines().anyMatch(l -> l.equals("crawl_signature=sig2")));
    }

    /**
     * The persisted string must round-trip through ParameterUtil.parse with the JSON intact. This
     * pins the reason the key is plural: "start_page_token" would match app.encrypt.property.pattern.
     */
    @Test
    public void test_buildHandlerParameter_roundTripsThroughDataConfig() {
        final CapturingState state = new CapturingState(newDataConfig("crawl_target=shared_drives"));
        state.putToken("drive:d1", "111");
        state.putToken("user:alice@example.com", "222");
        state.setSignature("sig1");
        final String result = state.buildHandlerParameter("crawl_target=shared_drives");

        final DataConfig reloaded = newDataConfig(result);
        final Map<String, String> parsed = reloaded.getHandlerParameterMap();
        assertEquals("{\"drive:d1\":\"111\",\"user:alice@example.com\":\"222\"}", parsed.get("start_page_tokens"));
        assertEquals("sig1", parsed.get("crawl_signature"));

        final DriveCrawlState reread = new CapturingState(reloaded);
        assertEquals("111", reread.getToken("drive:d1"));
        assertEquals("222", reread.getToken("user:alice@example.com"));
    }

    /**
     * save() is a no-op when nothing changed, and writes exactly once otherwise.
     */
    @Test
    public void test_save_onlyWhenDirty() {
        final String stored = "start_page_tokens={\"drive:d1\":\"111\"}\ncrawl_signature=sig1";
        final CapturingState state = new CapturingState(newDataConfig(stored));
        state.putToken("drive:d1", "111");
        state.setSignature("sig1");
        state.save();
        assertEquals("an unchanged state must not touch the config index", 0, state.updates.get());

        state.putToken("drive:d1", "222");
        state.save();
        assertEquals(1, state.updates.get());
        state.save();
        assertEquals("a second save after no change is a no-op", 1, state.updates.get());
    }

    /**
     * clearTokens and removeToken drop the stored tokens so the next run falls back to a full crawl.
     */
    @Test
    public void test_clearAndRemoveTokens() {
        final CapturingState state = new CapturingState(newDataConfig("start_page_tokens={\"drive:d1\":\"111\",\"drive:d2\":\"222\"}"));
        state.removeToken("drive:d1");
        assertNull(state.getToken("drive:d1"));
        assertEquals("222", state.getToken("drive:d2"));
        state.clearTokens();
        assertNull(state.getToken("drive:d2"));
        state.save();
        assertEquals(1, state.updates.get());
    }
}

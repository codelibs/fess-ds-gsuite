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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.opensearch.config.exbhv.DataConfigBhv;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Persists the Drive change feed start page tokens in the {@code handlerParameter} of a data config.
 * <p>
 * The change feed is scoped: one token per shared drive and one per impersonated user. The whole map
 * is stored as a single JSON line so that a run can resume every scope independently.
 * </p>
 */
public class DriveCrawlState {

    private static final Logger logger = LogManager.getLogger(DriveCrawlState.class);

    /**
     * The handler parameter key holding the scope key to start page token map, as JSON.
     * <p>
     * The name is deliberately plural. Fess encrypts a handler parameter whose key fully matches
     * {@code app.encrypt.property.pattern} ({@code .*password|.*key|.*token|.*secret}); the singular
     * {@code start_page_token} would match, the plural does not.
     * </p>
     */
    public static final String START_PAGE_TOKENS = "start_page_tokens";

    /** The handler parameter key holding the hash of the crawl scope configuration. */
    public static final String CRAWL_SIGNATURE = "crawl_signature";

    /** The prefix of a shared drive scope key. */
    protected static final String DRIVE_SCOPE_PREFIX = "drive:";

    /** The prefix of an impersonated user scope key. */
    protected static final String USER_SCOPE_PREFIX = "user:";

    /** The data configuration the state is stored in. May be null in unit tests. */
    protected final DataConfig dataConfig;

    /** The scope key to start page token map. */
    protected final Map<String, String> tokens = new LinkedHashMap<>();

    /** The hash of the crawl scope configuration of the previous run. */
    protected String signature;

    /** Whether anything changed since the state was loaded. */
    protected boolean dirty;

    /**
     * Loads the state from a data configuration.
     *
     * @param dataConfig The data configuration, or null when there is nothing to persist.
     */
    public DriveCrawlState(final DataConfig dataConfig) {
        this.dataConfig = dataConfig;
        if (dataConfig == null) {
            return;
        }
        final Map<String, String> paramMap = dataConfig.getHandlerParameterMap();
        signature = paramMap.get(CRAWL_SIGNATURE);
        final String json = paramMap.get(START_PAGE_TOKENS);
        if (StringUtil.isNotBlank(json)) {
            try {
                tokens.putAll(new ObjectMapper().readValue(json, new TypeReference<Map<String, String>>() {
                }));
            } catch (final Exception e) {
                // A corrupt state must degrade to a full crawl, never abort one.
                logger.warn("Failed to parse {}. Falling back to a full crawl.", START_PAGE_TOKENS, e);
                tokens.clear();
            }
        }
    }

    /**
     * Returns the stored start page token of a scope.
     *
     * @param scopeKey The scope key.
     * @return The token, or null when the scope has never been crawled.
     */
    public String getToken(final String scopeKey) {
        return tokens.get(scopeKey);
    }

    /**
     * Stores the start page token of a scope.
     *
     * @param scopeKey The scope key.
     * @param token The token to persist.
     */
    public void putToken(final String scopeKey, final String token) {
        if (scopeKey == null || token == null) {
            return;
        }
        final String previous = tokens.put(scopeKey, token);
        if (!token.equals(previous)) {
            dirty = true;
        }
    }

    /**
     * Drops the token of a single scope so that scope is fully resynchronized on the next run.
     *
     * @param scopeKey The scope key.
     */
    public void removeToken(final String scopeKey) {
        if (tokens.remove(scopeKey) != null) {
            dirty = true;
        }
    }

    /**
     * Drops every token so the whole configuration is fully resynchronized on the next run.
     */
    public void clearTokens() {
        if (!tokens.isEmpty()) {
            tokens.clear();
            dirty = true;
        }
    }

    /**
     * Returns whether the stored tokens were produced by the same crawl scope configuration.
     *
     * @param signature The signature of the current configuration.
     * @return true when the stored tokens may be reused.
     */
    public boolean isCompatible(final String signature) {
        return this.signature != null && this.signature.equals(signature);
    }

    /**
     * Sets the signature of the current crawl scope configuration.
     *
     * @param signature The signature.
     */
    public void setSignature(final String signature) {
        if (!Objects.equals(this.signature, signature)) {
            this.signature = signature;
            dirty = true;
        }
    }

    /**
     * Writes the state back into the data configuration when anything changed.
     */
    public void save() {
        if (dataConfig == null || !dirty) {
            return;
        }
        dataConfig.setHandlerParameter(buildHandlerParameter(dataConfig.getHandlerParameter()));
        updateDataConfig(dataConfig);
        dirty = false;
    }

    /**
     * Persists the data configuration. Separated so tests can capture the write.
     *
     * @param dataConfig The data configuration.
     */
    protected void updateDataConfig(final DataConfig dataConfig) {
        ComponentUtil.getComponent(DataConfigBhv.class).update(dataConfig);
        logger.info("Updated DataConfig: {}", dataConfig.getId());
    }

    /**
     * Rebuilds the {@code handlerParameter} string with the state applied, updating the two keys in
     * place when they already exist and appending them otherwise.
     * <p>
     * The <em>raw</em> stored string is edited line by line rather than rebuilt from
     * {@link DataConfig#getHandlerParameterMap()}: that map is already decrypted by
     * {@code ParameterUtil.parse}, so rebuilding from it would rewrite {@code private_key={cipher}...}
     * in cleartext on every crawl. Every other line is preserved verbatim; only blank lines are
     * dropped, which {@code ParameterUtil.parse} ignores anyway.
     * </p>
     *
     * @param handlerParameter The current raw handler parameter string, possibly null.
     * @return The rebuilt handler parameter string.
     */
    protected String buildHandlerParameter(final String handlerParameter) {
        final Map<String, String> pending = new LinkedHashMap<>();
        pending.put(START_PAGE_TOKENS, toJson());
        if (signature != null) {
            pending.put(CRAWL_SIGNATURE, signature);
        }

        final StringBuilder buf = new StringBuilder();
        if (handlerParameter != null) {
            for (final String line : handlerParameter.split("[\r\n]")) {
                if (StringUtil.isBlank(line)) {
                    continue;
                }
                final int pos = line.indexOf('=');
                final String key = (pos >= 0 ? line.substring(0, pos) : line).trim();
                if (buf.length() > 0) {
                    buf.append('\n');
                }
                if (pending.containsKey(key)) {
                    buf.append(key).append('=').append(pending.remove(key));
                } else {
                    buf.append(line);
                }
            }
        }
        pending.forEach((key, value) -> {
            if (buf.length() > 0) {
                buf.append('\n');
            }
            buf.append(key).append('=').append(value);
        });
        return buf.toString();
    }

    /**
     * Serializes the token map to a single JSON line.
     *
     * @return The JSON representation of the token map.
     */
    protected String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(tokens);
        } catch (final Exception e) {
            throw new DataStoreException("Failed to serialize " + START_PAGE_TOKENS + ".", e);
        }
    }

    /**
     * Returns the scope key of a shared drive.
     *
     * @param driveId The shared drive id.
     * @return The scope key.
     */
    public static String driveScopeKey(final String driveId) {
        return DRIVE_SCOPE_PREFIX + driveId;
    }

    /**
     * Returns the scope key of an impersonated user.
     *
     * @param userEmail The user email address.
     * @return The scope key.
     */
    public static String userScopeKey(final String userEmail) {
        return USER_SCOPE_PREFIX + userEmail;
    }
}

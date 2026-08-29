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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.util.BackOff;

/**
 * Tests for the exponential back-off and the retry classification of {@link GSuiteClient}.
 *
 * <p>
 * No test asserts on measured wall-clock time: the wait a failure produces is asserted through
 * {@link GSuiteClient#computeWaitMillis(long, HttpResponseException, long)}, which returns the
 * interval {@code executeWithRetry} would sleep for. The few tests that do drive
 * {@code executeWithRetry} end to end configure single-digit millisecond intervals, so the sleeps
 * they perform are negligible.
 * </p>
 */
public class GSuiteClientRetryTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * Builds a GoogleJsonResponseException with the given status code, error reason and Retry-After header.
     *
     * @param statusCode The HTTP status code.
     * @param reason The error reason, or null for no errors array.
     * @param retryAfter The Retry-After header value, or null.
     * @return The exception.
     */
    protected static GoogleJsonResponseException newJsonError(final int statusCode, final String reason, final String retryAfter) {
        final HttpHeaders headers = new HttpHeaders();
        if (retryAfter != null) {
            headers.setRetryAfter(retryAfter);
        }
        final GoogleJsonError error = new GoogleJsonError();
        error.setCode(statusCode);
        error.setMessage("test error");
        if (reason != null) {
            final GoogleJsonError.ErrorInfo info = new GoogleJsonError.ErrorInfo();
            info.setDomain("usageLimits");
            info.setReason(reason);
            info.setMessage("test error");
            error.setErrors(List.of(info));
        }
        final HttpResponseException.Builder builder = new HttpResponseException.Builder(statusCode, "test", headers);
        return new GoogleJsonResponseException(builder, error);
    }

    /**
     * Builds a plain HttpResponseException carrying an unparsed JSON body, as the Admin SDK REST
     * call raises: it goes through {@code HttpRequest} rather than through a generated Google client,
     * so it never becomes a {@link GoogleJsonResponseException}.
     *
     * @param statusCode The HTTP status code.
     * @param content The response body, or null.
     * @return The exception.
     */
    protected static HttpResponseException newRawError(final int statusCode, final String content) {
        return new HttpResponseException.Builder(statusCode, "test", new HttpHeaders()).setContent(content).build();
    }

    /**
     * The wait interval must follow min((2^n) * initial + jitter, maxBackOff) and stop after maxRetries.
     */
    @Test
    public void test_googleBackOff_followsGoogleFormula() throws Exception {
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(3, 1000L, 32000L, new Random(0L));
        final long first = backOff.nextBackOffMillis();
        assertTrue("first wait is 1000..1999", first >= 1000L && first < 2000L);
        final long second = backOff.nextBackOffMillis();
        assertTrue("second wait is 2000..2999", second >= 2000L && second < 3000L);
        final long third = backOff.nextBackOffMillis();
        assertTrue("third wait is 4000..4999", third >= 4000L && third < 5000L);
        assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    }

    /**
     * The maximum backoff must clamp the exponential growth.
     */
    @Test
    public void test_googleBackOff_clampsToMaxBackOff() throws Exception {
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(10, 1000L, 3000L, new Random(0L));
        backOff.nextBackOffMillis();
        backOff.nextBackOffMillis();
        assertEquals(3000L, backOff.nextBackOffMillis());
        assertEquals(3000L, backOff.nextBackOffMillis());
    }

    /**
     * reset() must restart the sequence.
     */
    @Test
    public void test_googleBackOff_reset() throws Exception {
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(2, 1000L, 32000L, new Random(0L));
        backOff.nextBackOffMillis();
        backOff.nextBackOffMillis();
        assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
        backOff.reset();
        assertTrue("after reset the sequence restarts", backOff.nextBackOffMillis() < 2000L);
    }

    /**
     * 429 / 500 / 502 / 503 / 504 are retryable regardless of the body.
     */
    @Test
    public void test_isRetryable_serverErrors() {
        assertTrue("429", GSuiteClient.isRetryable(newJsonError(429, null, null)));
        assertTrue("500", GSuiteClient.isRetryable(newJsonError(500, null, null)));
        assertTrue("502", GSuiteClient.isRetryable(newJsonError(502, null, null)));
        assertTrue("503", GSuiteClient.isRetryable(newJsonError(503, null, null)));
        assertTrue("504", GSuiteClient.isRetryable(newJsonError(504, null, null)));
    }

    /**
     * A 403 is retryable only when its reason is a quota reason. An authorization failure such as a
     * missing scope must surface immediately instead of burning the retry budget.
     */
    @Test
    public void test_isRetryable_403DependsOnReason() {
        assertTrue("userRateLimitExceeded", GSuiteClient.isRetryable(newJsonError(403, "userRateLimitExceeded", null)));
        assertTrue("rateLimitExceeded", GSuiteClient.isRetryable(newJsonError(403, "rateLimitExceeded", null)));
        assertFalse("insufficientFilePermissions", GSuiteClient.isRetryable(newJsonError(403, "insufficientFilePermissions", null)));
        assertFalse("insufficientPermissions", GSuiteClient.isRetryable(newJsonError(403, "insufficientPermissions", null)));
        assertFalse("dailyLimitExceeded is a daily quota, not a burst",
                GSuiteClient.isRetryable(newJsonError(403, "dailyLimitExceeded", null)));
        assertFalse("no errors array", GSuiteClient.isRetryable(newJsonError(403, null, null)));
    }

    /**
     * A 403 raised outside a generated Google client carries no parsed details, so the reason has to
     * be read out of the raw body. The match is on the quoted JSON value, so a longer reason that
     * merely ends with a retryable one does not count as retryable.
     */
    @Test
    public void test_isRetryable_403FromRawBody() {
        assertTrue("quota reason in the raw body",
                GSuiteClient.isRetryable(newRawError(403, "{\"error\":{\"errors\":[{\"reason\":\"userRateLimitExceeded\"}]}}")));
        assertFalse("authorization failure in the raw body",
                GSuiteClient.isRetryable(newRawError(403, "{\"error\":{\"errors\":[{\"reason\":\"insufficientPermissions\"}]}}")));
        assertFalse("sharingRateLimitExceeded merely ends with a retryable reason",
                GSuiteClient.isRetryable(newRawError(403, "{\"error\":{\"errors\":[{\"reason\":\"sharingRateLimitExceeded\"}]}}")));
        assertFalse("no body at all", GSuiteClient.isRetryable(newRawError(403, null)));
    }

    /**
     * 400 / 401 / 404 must never be retried.
     */
    @Test
    public void test_isRetryable_clientErrors() {
        assertFalse("400", GSuiteClient.isRetryable(newJsonError(400, null, null)));
        assertFalse("401", GSuiteClient.isRetryable(newJsonError(401, null, null)));
        assertFalse("404", GSuiteClient.isRetryable(newJsonError(404, null, null)));
    }

    /**
     * A numeric Retry-After is honoured; an HTTP-date or a missing header yields zero.
     */
    @Test
    public void test_getRetryAfterMillis() {
        assertEquals(7000L, GSuiteClient.getRetryAfterMillis(newJsonError(429, null, "7")));
        assertEquals(0L, GSuiteClient.getRetryAfterMillis(newJsonError(429, null, null)));
        assertEquals(0L, GSuiteClient.getRetryAfterMillis(newJsonError(429, null, "Wed, 21 Oct 2026 07:28:00 GMT")));
    }

    /**
     * The wait a failure produces: Retry-After wins over the exponential wait, but never exceeds the
     * maximum backoff and is never negative.
     */
    @Test
    public void test_computeWaitMillis() {
        assertEquals("Retry-After wins over the exponential wait", 7000L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, "7"), 32000L));
        assertEquals("a 600s Retry-After is clamped to the maximum backoff", 32000L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, "600"), 32000L));
        assertEquals("no Retry-After leaves the exponential wait alone", 1000L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, null), 32000L));
        assertEquals("an HTTP-date Retry-After falls back to the exponential wait", 1000L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, "Wed, 21 Oct 2026 07:28:00 GMT"), 32000L));
        assertEquals("a Retry-After shorter than the exponential wait does not shorten it", 4000L,
                GSuiteClient.computeWaitMillis(4000L, newJsonError(429, null, "1"), 32000L));
        assertEquals("a negative Retry-After cannot produce a negative wait", 1000L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, "-5"), 32000L));
        assertEquals("a misconfigured maximum backoff cannot produce a negative wait", 0L,
                GSuiteClient.computeWaitMillis(1000L, newJsonError(429, null, null), -1L));
    }

    /**
     * A retryable failure is retried until the call succeeds.
     */
    @Test
    public void test_executeWithRetry_retriesUntilSuccess() throws Exception {
        final AtomicInteger attempts = new AtomicInteger();
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(5, 1L, 4L, new Random(0L));
        final String result = GSuiteClient.executeWithRetry("test", backOff, () -> {
            if (attempts.incrementAndGet() < 3) {
                throw newJsonError(503, null, null);
            }
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(3, attempts.get());
    }

    /**
     * The retry budget is bounded: the last exception is rethrown once the back-off stops.
     */
    @Test
    public void test_executeWithRetry_givesUpAfterMaxRetries() {
        final AtomicInteger attempts = new AtomicInteger();
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(2, 1L, 4L, new Random(0L));
        try {
            GSuiteClient.executeWithRetry("test", backOff, () -> {
                attempts.incrementAndGet();
                throw newJsonError(429, null, null);
            });
            fail("Expected HttpResponseException");
        } catch (final IOException e) {
            assertTrue("the original status survives", e instanceof HttpResponseException);
            assertEquals(429, ((HttpResponseException) e).getStatusCode());
        }
        assertEquals("1 initial attempt + 2 retries", 3, attempts.get());
    }

    /**
     * A non-retryable failure is rethrown immediately without any retry.
     */
    @Test
    public void test_executeWithRetry_doesNotRetryClientError() {
        final AtomicInteger attempts = new AtomicInteger();
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(5, 1L, 4L, new Random(0L));
        try {
            GSuiteClient.executeWithRetry("test", backOff, () -> {
                attempts.incrementAndGet();
                throw newJsonError(404, null, null);
            });
            fail("Expected HttpResponseException");
        } catch (final IOException e) {
            assertEquals(404, ((HttpResponseException) e).getStatusCode());
        }
        assertEquals(1, attempts.get());
    }

    /**
     * An outrageous Retry-After does not stall the crawl: the back-off clamps it, so the call is
     * retried and succeeds instead of parking the thread for ten minutes.
     */
    @Test
    public void test_executeWithRetry_retryAfterIsClamped() throws Exception {
        final AtomicInteger attempts = new AtomicInteger();
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(3, 1L, 5L, new Random(0L));
        final String result = GSuiteClient.executeWithRetry("test", backOff, () -> {
            if (attempts.incrementAndGet() < 2) {
                throw newJsonError(429, null, "600");
            }
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(2, attempts.get());
        assertEquals("a 600s Retry-After is capped at the 5 ms maximum backoff", 5L,
                GSuiteClient.computeWaitMillis(1L, newJsonError(429, null, "600"), backOff.getMaxBackOffMillis()));
    }

    /**
     * A checked exception that is not an HTTP failure is never retried.
     */
    @Test
    public void test_executeWithRetry_doesNotRetryPlainIOException() {
        final AtomicInteger attempts = new AtomicInteger();
        final GSuiteClient.GoogleBackOff backOff = new GSuiteClient.GoogleBackOff(5, 1L, 4L, new Random(0L));
        try {
            GSuiteClient.executeWithRetry("test", backOff, () -> {
                attempts.incrementAndGet();
                throw new IOException("boom");
            });
            fail("Expected IOException");
        } catch (final IOException e) {
            assertEquals("boom", e.getMessage());
        }
        assertEquals(1, attempts.get());
    }

    /**
     * The retry parameters are read from the data store parameters, and a malformed value falls back
     * to the default instead of failing the crawl.
     */
    @Test
    public void test_getIntParam() {
        final DataStoreParams params = new DataStoreParams();
        assertEquals("absent", 5, GSuiteClient.getIntParam(params, GSuiteClient.MAX_RETRIES, 5));
        params.put(GSuiteClient.MAX_RETRIES, " 7 ");
        assertEquals("trimmed", 7, GSuiteClient.getIntParam(params, GSuiteClient.MAX_RETRIES, 5));
        params.put(GSuiteClient.MAX_RETRIES, "  ");
        assertEquals("blank", 5, GSuiteClient.getIntParam(params, GSuiteClient.MAX_RETRIES, 5));
        params.put(GSuiteClient.MAX_RETRIES, "abc");
        assertEquals("malformed", 5, GSuiteClient.getIntParam(params, GSuiteClient.MAX_RETRIES, 5));
    }
}

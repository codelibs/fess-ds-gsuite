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
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;

/**
 * A {@link MockHttpTransport} that replays queued responses in FIFO order and records requested URLs.
 */
public class MockDriveTransport extends MockHttpTransport {

    /** Queued responses, replayed in FIFO order. */
    private final ConcurrentLinkedQueue<MockLowLevelHttpResponse> responses = new ConcurrentLinkedQueue<>();

    /** URLs that were actually requested, in order. */
    private final List<String> requestedUrls = new ArrayList<>();

    /**
     * Queues a response.
     *
     * @param statusCode The HTTP status code.
     * @param contentType The Content-Type header value.
     * @param body The response body.
     */
    public void queue(final int statusCode, final String contentType, final String body) {
        responses.add(new MockLowLevelHttpResponse().setStatusCode(statusCode).setContentType(contentType).setContent(body));
    }

    /**
     * Queues a 200 JSON response.
     *
     * @param body The JSON body.
     */
    public void queueJson(final String body) {
        queue(200, "application/json; charset=UTF-8", body);
    }

    /**
     * Returns the URLs that were requested, in order.
     *
     * @return The requested URLs.
     */
    public List<String> getRequestedUrls() {
        synchronized (requestedUrls) {
            return new ArrayList<>(requestedUrls);
        }
    }

    @Override
    public LowLevelHttpRequest buildRequest(final String method, final String url) {
        synchronized (requestedUrls) {
            requestedUrls.add(url);
        }
        return new MockLowLevelHttpRequest(url) {
            @Override
            public LowLevelHttpResponse execute() {
                final MockLowLevelHttpResponse response = responses.poll();
                if (response == null) {
                    throw new IllegalStateException("No queued response for " + method + " " + url);
                }
                return response;
            }
        };
    }
}

/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.timer.TimeoutManager;
import org.codelibs.core.timer.TimeoutTarget;
import org.codelibs.core.timer.TimeoutTask;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.exception.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.UrlEncodedContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport.Builder;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files.List;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

public class GSuiteClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GSuiteClient.class);

    protected static final String PRIVATE_KEY_PARAM = "private_key";
    protected static final String PRIVATE_KEY_ID_PARAM = "private_key_id";
    protected static final String CLIENT_EMAIL_PARAM = "client_email";
    protected static final String PROXY_PORT = "proxy_port";
    protected static final String PROXY_HOST = "proxy_host";
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";

    protected Drive drive;
    protected NetHttpTransport httpTransport;
    protected Map<String, String> params;

    protected int maxCachedContentSize = 1024 * 1024;

    protected RequestInitializer requestInitializer;

    protected TimeoutTask refreshTokenTask;

    protected String applicationName = "Fess DataStore";

    public GSuiteClient(final Map<String, String> params) {
        this.params = params;
        this.httpTransport = newHttpTransport();
        final String size = params.get(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
        requestInitializer = new RequestInitializer(params, httpTransport);
        refreshTokenTask = TimeoutManager.getInstance().addTimeoutTarget(requestInitializer,
                Integer.parseInt(params.getOrDefault(REFRESH_TOKEN_INTERVAL, "3540")), true);
    }

    @Override
    public void close() {
        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }
    }

    protected NetHttpTransport newHttpTransport() {
        try {
            final Builder builder = new NetHttpTransport.Builder().trustCertificates(GoogleUtils.getCertificateTrustStore());
            final String proxyHost = params.get(PROXY_HOST);
            final String proxyPort = params.get(PROXY_PORT);
            if (StringUtil.isNotBlank(proxyHost) && StringUtil.isNotBlank(proxyPort)) {
                builder.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort))));
            }
            return builder.build();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a http transport.", e);
        }
    }

    protected Drive createGlobalDrive() {
        return new Drive.Builder(httpTransport, new JacksonFactory(), requestInitializer)//
                .setApplicationName(applicationName) //
                .build();
    }

    protected Drive getDrive() {
        if (drive == null) {
            drive = createGlobalDrive();
        }
        return drive;
    }

    public void getFiles(final String q, final String corpora, final String spaces, final String fields, final Consumer<File> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("query: {}, corpora: {}, spaces: {}, fields: {}", q, corpora, spaces, fields);
        }
        long counter = 1;
        String pageToken = null;
        try {
            do {
                final List list = getDrive().files().list().setPageToken(pageToken);
                if (StringUtil.isNotBlank(q)) {
                    list.setQ(q);
                }
                if (StringUtil.isNotBlank(fields)) {
                    list.setFields(fields);
                }
                if (StringUtil.isNotBlank(corpora)) {
                    list.setCorpora(corpora);
                }
                if (corpora == "allDrives") {
                    list.setIncludeTeamDriveItems(true);
                    list.setSupportsTeamDrives(true);
                }
                if (StringUtil.isNotBlank(spaces)) {
                    list.setSpaces(spaces);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Accessing files: {}=>{}", counter, pageToken);
                }
                final FileList result = list.execute();
                if (logger.isDebugEnabled()) {
                    logger.debug("filelist: {}", result);
                }
                for (final File file : result.getFiles()) {
                    consumer.accept(file);
                }
                pageToken = result.getNextPageToken();
                counter++;
            } while (pageToken != null);
        } catch (final IOException e) {
            throw new DataStoreException("Failed to access files.", e);
        }
    }

    public String extractFileText(final String id, final String mimeType) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            getDrive().files().export(id, mimeType).executeMediaAndDownloadTo(out);
            return out.toString(Constants.UTF_8);
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to extract a text from " + id, e);
        }
    }

    public InputStream getFileInputStream(final String id) {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-GSuiteClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            getDrive().files().get(id).executeMediaAndDownloadTo(dfos);
            dfos.flush();

            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            } else {
                return new TemporaryFileInputStream(dfos.getFile());
            }
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + id, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class TokenResponse {
        @JsonProperty("access_token")
        private String accessToken;
        @JsonProperty("expires_in")
        private Integer expiresIn;
        @JsonProperty("token_type")
        private String tokenType;
        private String error;
        @JsonProperty("error_description")
        private String errorDescription;

        String getAccessToken() {
            return accessToken;
        }

        Integer getExpiresIn() {
            return expiresIn;
        }

        String getTokenType() {
            return tokenType;
        }

        String getError() {
            return error;
        }

        String getErrorDescription() {
            return errorDescription;
        }
    }

    protected static class RequestInitializer implements HttpRequestInitializer, TimeoutTarget {

        protected NetHttpTransport httpTransport;

        protected String privateKeyPem;
        protected String privateKeyId;
        protected String clientEmail;
        protected String accessToken;

        protected RequestInitializer(final Map<String, String> params, final NetHttpTransport httpTransport) {
            this.httpTransport = httpTransport;

            privateKeyPem = params.getOrDefault(PRIVATE_KEY_PARAM, StringUtil.EMPTY);
            privateKeyId = params.getOrDefault(PRIVATE_KEY_ID_PARAM, StringUtil.EMPTY);
            clientEmail = params.getOrDefault(CLIENT_EMAIL_PARAM, StringUtil.EMPTY);
            if (privateKeyPem.isEmpty() || privateKeyId.isEmpty() || clientEmail.isEmpty()) {
                throw new DataStoreException("parameter '" + //
                        PRIVATE_KEY_PARAM + "', '" + //
                        PRIVATE_KEY_ID_PARAM + "', '" + //
                        CLIENT_EMAIL_PARAM + "' is required");
            }

            refreshToken();
        }

        protected void refreshToken() {
            if (httpTransport == null) {
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Refreshing access token.");
            }
            final long now = System.currentTimeMillis();
            try {
                final String jwt = JWT.create() //
                        .withKeyId(privateKeyId) //
                        .withIssuer(clientEmail) //
                        .withSubject(clientEmail) //
                        .withAudience("https://www.googleapis.com/oauth2/v4/token") //
                        .withClaim("scope", "https://www.googleapis.com/auth/drive") //
                        .withIssuedAt(new Date(now)) //
                        .withExpiresAt(new Date(now + 3600000L)) //
                        .sign(Algorithm.RSA256(null, (RSAPrivateKey) getPrivateKey()));
                if (logger.isDebugEnabled()) {
                    logger.debug("jwt: {}", jwt);
                }
                final GenericUrl url = new GenericUrl("https://www.googleapis.com/oauth2/v4/token");
                final GenericData data = new GenericData();
                data.set("assertion", jwt);
                data.set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
                final HttpContent content = new UrlEncodedContent(data);
                final HttpResponse response = httpTransport.createRequestFactory().buildPostRequest(url, content).execute();
                if (logger.isDebugEnabled()) {
                    logger.debug("response: {}", response);
                }
                try {
                    final ObjectMapper mapper = new ObjectMapper();
                    final TokenResponse token = mapper.readValue(response.getContent(), TokenResponse.class);

                    if (logger.isDebugEnabled()) {
                        logger.debug("Update: {} -> {}", accessToken, token.getAccessToken());
                    }
                    accessToken = token.getAccessToken();
                } finally {
                    response.disconnect();
                }
            } catch (final Exception e) {
                throw new DataStoreException("Failed to authorize GSuite API.", e);
            }
        }

        protected PrivateKey getPrivateKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
            final String replaced = privateKeyPem.replaceAll("\\\\n|\\n|-----[A-Z ]+-----", StringUtil.EMPTY);
            final byte[] bytes = Base64.getDecoder().decode(replaced);
            final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
            final KeyFactory keyFactory = SecurityUtils.getRsaKeyFactory();
            return keyFactory.generatePrivate(keySpec);
        }

        @Override
        public void expired() {
            try {
                refreshToken();
            } catch (final Exception e) {
                logger.warn("Failed to refresh an access token.", e);
            }
        }

        @Override
        public void initialize(final HttpRequest request) throws IOException {
            request.getHeaders().setAuthorization("Bearer " + accessToken);
        }

    }

    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }
}

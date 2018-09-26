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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.UrlEncodedContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.PemReader;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class GSuiteDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(GSuiteDataStore.class);

    // parameters
    private static final String PROJECT_ID_PARAM = "project_id";
    private static final String PRIVATE_KEY_PARAM = "private_key";
    private static final String PRIVATE_KEY_ID_PARAM = "private_key_id";
    private static final String CLIENT_EMAIL_PARAM = "client_email";

    // scripts
    protected static final String MESSAGE = "message";

    protected String getName() {
        return "G Suite";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final String projectId = paramMap.getOrDefault(PROJECT_ID_PARAM, "");
        final String privateKeyPem = paramMap.getOrDefault(PRIVATE_KEY_PARAM, "");
        final String privateKeyId = paramMap.getOrDefault(PRIVATE_KEY_ID_PARAM, "");
        final String clientEmail = paramMap.getOrDefault(CLIENT_EMAIL_PARAM, "");

        if (projectId.isEmpty() || privateKeyPem.isEmpty() || privateKeyId.isEmpty() || clientEmail.isEmpty()) {
            logger.warn("parameter '" + //
                    PROJECT_ID_PARAM + "', '" + //
                    PRIVATE_KEY_PARAM + "', '" + //
                    PRIVATE_KEY_ID_PARAM + "', '" + //
                    CLIENT_EMAIL_PARAM + "' is required");
            return;
        }

        final PrivateKey privateKey;
        try {
            privateKey = getPrivateKey(privateKeyPem);
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            logger.warn("Failed to get '" + PRIVATE_KEY_PARAM + "'", e);
            return;
        }

        final Drive drive;
        try {
            drive = getDriveService(projectId, privateKey, privateKeyId, clientEmail);
        } catch (final IOException e) {
            logger.warn("Failed to get Drive Service", e);
            return;
        }

    }

    protected static PrivateKey getPrivateKey(final String privateKeyPem)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        final Reader reader = new StringReader(privateKeyPem);
        final PemReader.Section section = PemReader.readFirstSectionAndClose(reader, "PRIVATE KEY");
        final byte[] bytes = section.getBase64DecodedBytes();
        final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
        final KeyFactory keyFactory = SecurityUtils.getRsaKeyFactory();
        return keyFactory.generatePrivate(keySpec);
    }

    protected static Drive getDriveService(final String projectId, final PrivateKey privateKey, final String privateKeyId,
            final String clientEmail) throws IOException {
        final GoogleCredential credential = new GoogleCredential.Builder() //
                .setTransport(new NetHttpTransport()).setJsonFactory(new JacksonFactory()).setServiceAccountId(clientEmail)
                .setServiceAccountScopes(Collections.emptyList()).setServiceAccountPrivateKey(privateKey)
                .setServiceAccountPrivateKeyId(privateKeyId).setTokenServerEncodedUrl("https://oauth2.googleapis.com/token")
                .setServiceAccountProjectId(projectId).build();

        final long now = System.currentTimeMillis();

        final String jwt = JWT.create() //
                .withKeyId(privateKeyId) //
                .withIssuer(clientEmail) //
                .withSubject(clientEmail) //
                .withAudience("https://www.googleapis.com/oauth2/v4/token") //
                .withClaim("scope", "https://www.googleapis.com/auth/drive") //
                .withIssuedAt(new Date(now)) //
                .withExpiresAt(new Date(now + 3600 * 1000L)) //
                .sign(Algorithm.RSA256(null, (RSAPrivateKey) privateKey));

        final GenericUrl url = new GenericUrl("https://www.googleapis.com/oauth2/v4/token");
        final GenericData data = new GenericData();
        data.set("assertion", jwt);
        data.set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
        final HttpContent content = new UrlEncodedContent(data);
        final HttpResponse response = new NetHttpTransport().createRequestFactory().buildPostRequest(url, content).execute();

        final ObjectMapper mapper = new ObjectMapper();
        final TokenResponse token = mapper.readValue(response.getContent(), TokenResponse.class);

        return new Drive.Builder(new NetHttpTransport(), new JacksonFactory(),
                request -> request.getHeaders().setAuthorization("Bearer " + token.getAccessToken())) //
                .setApplicationName("Fess DataStore") //
                .build();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class TokenResponse {
        @JsonProperty("access_token")
        private String accessToken;
        @JsonProperty("expires_in")
        private Integer expiresIn;
        @JsonProperty("token_type")
        private String tokenType;
        private String error;
        @JsonProperty("error_description")
        private String errorDescription;

        public String getAccessToken() {
            return accessToken;
        }

        public Integer getExpiresIn() {
            return expiresIn;
        }

        public String getTokenType() {
            return tokenType;
        }

        public String getError() {
            return error;
        }

        public String getErrorDescription() {
            return errorDescription;
        }
    }

}

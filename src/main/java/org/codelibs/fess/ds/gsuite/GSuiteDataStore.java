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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.UrlEncodedContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GSuiteDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(GSuiteDataStore.class);

    // parameters
    private static final String PROJECT_ID_PARAM = "project_id";
    private static final String PRIVATE_KEY_PARAM = "private_key";
    private static final String PRIVATE_KEY_ID_PARAM = "private_key_id";
    private static final String CLIENT_EMAIL_PARAM = "client_email";

    // scripts
    private static final String FILES = "files";
    private static final String FILES_NAME = "name";
    private static final String FILES_DESCRIPTION = "description";
    private static final String FILES_CONTENTS = "contents";
    private static final String FILES_MIMETYPE = "mimetype";
    private static final String FILES_THUMBNAIL_LINK = "thumbnail_link";
    private static final String FILES_WEB_VIEW_LINK = "web_view_link";
    private static final String FILES_CREATED_TIME = "created_time";
    private static final String FILES_MODIFIED_TIME = "modified_time";

    // other
    private static final String[] FILES_FIELDS =
            { "files/id", "files/name", "files/description", "files/mimeType", "files/thumbnailLink", "files/webViewLink",
                    "files/createdTime", "files/modifiedTime" };

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
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
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

        storeFiles(dataConfig, callback, paramMap, scriptMap, defaultDataMap, drive);

    }

    protected void storeFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Drive drive) {
        try {
            drive.files().list().setFields(String.join(",", FILES_FIELDS)).execute().getFiles().forEach(file -> {
                processFile(dataConfig, callback, paramMap, scriptMap, defaultDataMap, drive, file);
            });
        } catch (final IOException e) {
            logger.warn("Failed to store files", e);
        }
    }

    protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Drive drive, final File file) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> fileMap = new HashMap<>();

        try {
            fileMap.put(FILES_NAME, file.getName());
            fileMap.put(FILES_DESCRIPTION, file.getDescription());
            fileMap.put(FILES_CONTENTS, getFileContents(drive, file));
            fileMap.put(FILES_MIMETYPE, file.getMimeType());
            fileMap.put(FILES_THUMBNAIL_LINK, file.getThumbnailLink());
            fileMap.put(FILES_WEB_VIEW_LINK, file.getWebViewLink());
            fileMap.put(FILES_CREATED_TIME, file.getCreatedTime());
            fileMap.put(FILES_MODIFIED_TIME, file.getModifiedTime());
            resultMap.put(FILES, fileMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);
        }
    }

    protected static String getFileContents(final Drive drive, final File file) {
        final StringBuilder sb = new StringBuilder();
        final String id = file.getId();
        final String mimeType = file.getMimeType();
        final Matcher m = Pattern.compile("application/vnd\\.google-apps\\.(.*)").matcher(mimeType);
        if (m.matches()) {
            try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                switch (m.group(1)) {
                case "document":
                case "presentation": {
                    drive.files().export(id, "text/plain").executeMediaAndDownloadTo(out);
                    sb.append(out.toString());
                    break;
                }
                case "spreadsheet": {
                    drive.files().export(id, "text/csv").executeMediaAndDownloadTo(out);
                    sb.append(out.toString());
                    break;
                }
                case "script": {
                    drive.files().export(id, "application/vnd.google-apps.script+json").executeMediaAndDownloadTo(out);
                    final Map<String, Object> map = new ObjectMapper().readValue(out.toString(), new TypeReference<Map<String, Object>>() {
                    });
                    if (map.containsKey("files")) {
                        @SuppressWarnings("unchecked")
                        final List<Map<String, Object>> files = (List<Map<String, Object>>) map.get("files");
                        files.forEach(f -> {
                            sb.append(f.getOrDefault("name", ""));
                            sb.append("\n");
                            sb.append(f.getOrDefault("source", ""));
                        });
                    }
                    break;
                }
                }
            } catch (final IOException e) {
                logger.warn("Failed to get contents of '" + file.getName() + "'", e);
            }
        } else {
            if (mimeType.matches("text/.*")) {
                try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    drive.files().get(id).executeMediaAndDownloadTo(out);
                    sb.append(out.toString());
                } catch (final IOException e) {
                    logger.warn("Failed to get contents of '" + file.getName() + "'", e);
                }
            }
        }
        return sb.toString();
    }

    protected static PrivateKey getPrivateKey(final String privateKeyPem) throws NoSuchAlgorithmException, InvalidKeySpecException {
        final String replaced = privateKeyPem.replaceAll("\\\\n", "").replaceAll("-----[A-Z ]+-----", "");
        final byte[] bytes = Base64.getDecoder().decode(replaced);
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

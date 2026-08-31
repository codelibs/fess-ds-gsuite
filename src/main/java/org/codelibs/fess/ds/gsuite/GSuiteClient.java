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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport.Builder;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files.List;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

/**
 * A client for accessing Google Suite APIs.
 */
public class GSuiteClient implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(GSuiteClient.class);

    /** Parameter key for the private key. */
    protected static final String PRIVATE_KEY_PARAM = "private_key";
    /** Parameter key for the private key ID. */
    protected static final String PRIVATE_KEY_ID_PARAM = "private_key_id";
    /** Parameter key for the client email. */
    protected static final String CLIENT_EMAIL_PARAM = "client_email";
    /** Parameter key for the read timeout. */
    protected static final String READ_TIMEOUT = "read_timeout";
    /** Parameter key for the connect timeout. */
    protected static final String CONNECT_TIMEOUT = "connect_timeout";
    /** Parameter key for the proxy port. */
    protected static final String PROXY_PORT = "proxy_port";
    /** Parameter key for the proxy host. */
    protected static final String PROXY_HOST = "proxy_host";
    /** Parameter key for the refresh token interval. */
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    /** Parameter key for the maximum cached content size. */
    protected static final String MAX_CACHED_CONTENT_SIZE = "max_cached_content_size";
    /** Parameter key for the OAuth scopes. */
    protected static final String SCOPES = "scopes";
    /** Parameter key for the user to impersonate via domain-wide delegation. */
    protected static final String IMPERSONATE_USER = "impersonate_user";
    /** Default OAuth scopes. Read-only, unlike the previous hardcoded read-write scope. */
    protected static final String DEFAULT_SCOPES = "https://www.googleapis.com/auth/drive.readonly";

    /** Constant for all drives. */
    public static final String ALL_DRIVES = "allDrives";

    /** Default maximum cached content size in bytes (1MB). */
    protected static final int DEFAULT_MAX_CACHED_CONTENT_SIZE = 1024 * 1024;

    /** Default refresh token interval in seconds (59 minutes). */
    protected static final String DEFAULT_REFRESH_TOKEN_INTERVAL = "3540";

    /** Default read timeout in milliseconds (20 seconds). */
    protected static final int DEFAULT_READ_TIMEOUT_MS = 20 * 1000;

    /** Default connect timeout in milliseconds (20 seconds). */
    protected static final int DEFAULT_CONNECT_TIMEOUT_MS = 20 * 1000;

    /** JWT token validity duration in milliseconds (1 hour), kept for API compatibility. */
    protected static final long JWT_TOKEN_VALIDITY_MS = 3600000L;

    /** Pattern for cleaning up PEM-encoded private keys (removes headers, footers, and newlines). */
    protected static final String PEM_CLEANUP_PATTERN = "\\\\n|\\n|-----[A-Z ]+-----";

    /** The Google Drive client. */
    protected Drive drive;
    /** The HTTP transport. */
    protected HttpTransport httpTransport;
    /** The data store parameters. */
    protected DataStoreParams params;

    /** The maximum size of content to be cached in memory. */
    protected int maxCachedContentSize = DEFAULT_MAX_CACHED_CONTENT_SIZE;

    /** The credentials for the service account. */
    protected GoogleCredentials credentials;
    /** The request initializer. */
    protected HttpRequestInitializer requestInitializer;
    /** The read timeout in milliseconds. */
    protected int readTimeout = DEFAULT_READ_TIMEOUT_MS;
    /** The connect timeout in milliseconds. */
    protected int connectTimeout = DEFAULT_CONNECT_TIMEOUT_MS;

    /** The name of the application. */
    protected String applicationName = "Fess DataStore";

    /**
     * Constructs a new GSuiteClient.
     *
     * @param params The data store parameters.
     */
    public GSuiteClient(final DataStoreParams params) {
        this(params, null);
    }

    /**
     * Constructs a new GSuiteClient with the given transport.
     *
     * @param params The data store parameters.
     * @param httpTransport The HTTP transport, or {@code null} to create a new one.
     */
    protected GSuiteClient(final DataStoreParams params, final HttpTransport httpTransport) {
        this.params = params;
        this.httpTransport = httpTransport != null ? httpTransport : newHttpTransport();
        final String size = params.getAsString(MAX_CACHED_CONTENT_SIZE);
        if (StringUtil.isNotBlank(size)) {
            maxCachedContentSize = Integer.parseInt(size);
        }
        final String readTimeoutStr = params.getAsString(READ_TIMEOUT);
        if (StringUtil.isNotBlank(readTimeoutStr)) {
            readTimeout = Integer.parseInt(readTimeoutStr);
        }
        final String connectTimeoutStr = params.getAsString(CONNECT_TIMEOUT);
        if (StringUtil.isNotBlank(connectTimeoutStr)) {
            connectTimeout = Integer.parseInt(connectTimeoutStr);
        }
        if (StringUtil.isNotBlank(params.getAsString(REFRESH_TOKEN_INTERVAL))) {
            logger.warn("{} is no longer used. Access tokens are refreshed by google-auth-library.", REFRESH_TOKEN_INTERVAL);
        }
        credentials = createCredentials();
        requestInitializer = createRequestInitializer(credentials);
    }

    @Override
    public void close() {
        // nothing to release
    }

    /**
     * Creates a new NetHttpTransport.
     * @return A new NetHttpTransport.
     */
    protected NetHttpTransport newHttpTransport() {
        try {
            final Builder builder = new NetHttpTransport.Builder().trustCertificates(GoogleUtils.getCertificateTrustStore());
            final String proxyHost = params.getAsString(PROXY_HOST);
            final String proxyPort = params.getAsString(PROXY_PORT);
            if (StringUtil.isNotBlank(proxyHost) && StringUtil.isNotBlank(proxyPort)) {
                builder.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort))));
            }
            return builder.build();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a http transport.", e);
        }
    }

    /**
     * Returns the OAuth scopes.
     * A blank (absent, empty, or whitespace-only) {@link #SCOPES} parameter falls back to
     * {@link #DEFAULT_SCOPES}. If the parameter is present and non-blank but, after splitting on
     * commas, trimming, and dropping blank entries, yields no usable scope (e.g. {@code ","}),
     * a {@link DataStoreException} is thrown instead of silently returning an empty collection.
     *
     * @return The OAuth scopes.
     */
    protected Collection<String> getScopes() {
        final String rawScopes = params.getAsString(SCOPES);
        final String scopesValue = StringUtil.isBlank(rawScopes) ? DEFAULT_SCOPES : rawScopes;
        final Collection<String> scopes =
                Arrays.stream(scopesValue.split(",")).map(String::trim).filter(StringUtil::isNotBlank).collect(Collectors.toList());
        if (scopes.isEmpty()) {
            throw new DataStoreException("parameter '" + SCOPES + "' must specify at least one scope");
        }
        return scopes;
    }

    /**
     * Creates credentials for the service account.
     *
     * @return The credentials.
     */
    protected GoogleCredentials createCredentials() {
        final String privateKeyPem = params.getAsString(PRIVATE_KEY_PARAM, StringUtil.EMPTY);
        final String privateKeyId = params.getAsString(PRIVATE_KEY_ID_PARAM, StringUtil.EMPTY);
        final String clientEmail = params.getAsString(CLIENT_EMAIL_PARAM, StringUtil.EMPTY);
        if (privateKeyPem.isEmpty() || privateKeyId.isEmpty() || clientEmail.isEmpty()) {
            throw new DataStoreException("parameter '" + //
                    PRIVATE_KEY_PARAM + "', '" + //
                    PRIVATE_KEY_ID_PARAM + "', '" + //
                    CLIENT_EMAIL_PARAM + "' is required");
        }
        try {
            final ServiceAccountCredentials serviceAccountCredentials = ServiceAccountCredentials.newBuilder()
                    .setClientEmail(clientEmail)
                    .setPrivateKeyId(privateKeyId)
                    .setPrivateKey(parsePrivateKey(privateKeyPem))
                    .setScopes(getScopes())
                    .setHttpTransportFactory(() -> httpTransport)
                    .build();
            final String impersonateUser = params.getAsString(IMPERSONATE_USER);
            if (StringUtil.isNotBlank(impersonateUser)) {
                return serviceAccountCredentials.createDelegated(impersonateUser);
            }
            return serviceAccountCredentials;
        } catch (final DataStoreException e) {
            throw e;
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create credentials for " + clientEmail, e);
        }
    }

    /**
     * Creates a request initializer that attaches the OAuth token and the configured timeouts.
     *
     * @param googleCredentials The credentials.
     * @return The request initializer.
     */
    protected HttpRequestInitializer createRequestInitializer(final GoogleCredentials googleCredentials) {
        final HttpCredentialsAdapter adapter = new HttpCredentialsAdapter(googleCredentials);
        return request -> {
            adapter.initialize(request);
            request.setReadTimeout(readTimeout);
            request.setConnectTimeout(connectTimeout);
        };
    }

    /**
     * Parses a PEM-encoded PKCS8 private key.
     *
     * @param privateKeyPem The PEM text. Both escaped and real newlines are accepted.
     * @return The private key.
     * @throws NoSuchAlgorithmException If RSA is unavailable.
     * @throws InvalidKeySpecException If the key is invalid.
     */
    protected static PrivateKey parsePrivateKey(final String privateKeyPem) throws NoSuchAlgorithmException, InvalidKeySpecException {
        try {
            final String replaced = privateKeyPem.replaceAll(PEM_CLEANUP_PATTERN, StringUtil.EMPTY).trim();
            if (replaced.isEmpty()) {
                throw new IllegalArgumentException("Private key content is empty after removing PEM headers");
            }
            final byte[] bytes = Base64.getDecoder().decode(replaced);
            if (bytes.length == 0) {
                throw new IllegalArgumentException("Decoded private key has zero length");
            }
            final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
            final KeyFactory keyFactory = SecurityUtils.getRsaKeyFactory();
            return keyFactory.generatePrivate(keySpec);
        } catch (final IllegalArgumentException e) {
            throw new InvalidKeySpecException("Failed to decode private key: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a new Drive client.
     *
     * @return A new Drive client.
     */
    protected Drive createGlobalDrive() {
        return new Drive.Builder(httpTransport, GsonFactory.getDefaultInstance(), requestInitializer)//
                .setApplicationName(applicationName) //
                .build();
    }

    /**
     * Returns the Drive client.
     * @return The Drive client.
     */
    protected Drive getDrive() {
        if (drive == null) {
            drive = createGlobalDrive();
        }
        return drive;
    }

    /**
     * Retrieves files from Google Drive.
     * @param q The query to search for files.
     * @param corpora The corpora to search in.
     * @param spaces The spaces to search in.
     * @param fields The fields to retrieve for each file.
     * @param consumer A consumer for each file.
     */
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
                if (ALL_DRIVES.equals(corpora)) {
                    list.setIncludeItemsFromAllDrives(true);
                    list.setSupportsAllDrives(true);
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

    /**
     * Extracts the text from a file.
     * <p>
     * Note that {@code files.export} has no {@code supportsAllDrives} parameter in the Drive v3
     * API, so unlike {@code files.get} this request cannot opt into shared drive support.
     * </p>
     * @param id The ID of the file.
     * @param mimeType The mime type of the file.
     * @return The text of the file.
     */
    public String extractFileText(final String id, final String mimeType) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            getDrive().files().export(id, mimeType).executeMediaAndDownloadTo(out);
            return out.toString(Constants.UTF_8);
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to extract a text from " + id, e);
        }
    }

    /**
     * Returns an input stream for a file.
     * @param id The ID of the file.
     * @return An input stream for the file.
     */
    public InputStream getFileInputStream(final String id) {
        try (final DeferredFileOutputStream dfos =
                new DeferredFileOutputStream(maxCachedContentSize, "crawler-GSuiteClient-", ".out", SystemUtils.getJavaIoTmpDir())) {
            newFileGetRequest(id).executeMediaAndDownloadTo(dfos);
            dfos.flush();

            if (dfos.isInMemory()) {
                return new ByteArrayInputStream(dfos.getData());
            }
            return new TemporaryFileInputStream(dfos.getFile());
        } catch (final Exception e) {
            throw new CrawlingAccessException("Failed to create an input stream from " + id, e);
        }
    }

    /**
     * Creates a files.get request for the given file.
     * <p>
     * {@code supportsAllDrives=true} is required for any item that lives on a shared drive;
     * without it Drive rejects the request for those items.
     * </p>
     * @param id The ID of the file.
     * @return The files.get request.
     * @throws IOException If the request cannot be created.
     */
    protected Drive.Files.Get newFileGetRequest(final String id) throws IOException {
        return getDrive().files().get(id).setSupportsAllDrives(Boolean.TRUE);
    }

    /**
     * Sets the application name.
     * @param applicationName The application name.
     */
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }
}

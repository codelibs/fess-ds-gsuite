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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport.Builder;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.DriveList;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.drive.model.PermissionList;
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
    /** The Admin SDK scope that {@code admin/directory/v1/users} requires. Not part of {@link #DEFAULT_SCOPES}. */
    protected static final String ADMIN_DIRECTORY_USER_READONLY_SCOPE = "https://www.googleapis.com/auth/admin.directory.user.readonly";

    /** Constant for all drives. */
    public static final String ALL_DRIVES = "allDrives";

    /** Corpora value that scopes files.list to a single shared drive. */
    public static final String DRIVE_CORPORA = "drive";

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

    /** The maximum page size accepted by permissions.list. The API caps this at 100. */
    protected static final int PERMISSION_PAGE_SIZE_LIMIT = 100;

    /** The field projection used by permissions.list. */
    protected static final String PERMISSION_FIELDS =
            "nextPageToken,permissions(id,type,role,emailAddress,domain,deleted,allowFileDiscovery,permissionDetails)";

    /** The maximum page size accepted by drives.list. The API caps this at 100, unlike files.list. */
    protected static final int DRIVE_PAGE_SIZE_LIMIT = 100;

    /** The field projection used by drives.list. */
    protected static final String DRIVE_FIELDS = "nextPageToken,drives(id,name)";

    /** The Admin SDK Directory endpoint that lists users. */
    protected static final String ADMIN_DIRECTORY_USERS_URL = "https://admin.googleapis.com/admin/directory/v1/users";

    /** The customer alias that resolves to the account of the impersonated administrator. */
    protected static final String ADMIN_CUSTOMER = "my_customer";

    /** The maximum page size accepted by Admin SDK users.list. The API caps this at 500. */
    protected static final int ADMIN_MAX_RESULTS = 500;

    /** The field projection used by Admin SDK users.list. */
    protected static final String ADMIN_USER_FIELDS = "nextPageToken,users(primaryEmail)";

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
     * Returns the OAuth scopes of this client.
     *
     * @return The OAuth scopes.
     * @see #resolveScopes(DataStoreParams)
     */
    protected Collection<String> getScopes() {
        return resolveScopes(params);
    }

    /**
     * Resolves the OAuth scopes carried by the given parameters.
     * A blank (absent, empty, or whitespace-only) {@link #SCOPES} parameter falls back to
     * {@link #DEFAULT_SCOPES}. If the parameter is present and non-blank but, after splitting on
     * commas, trimming, and dropping blank entries, yields no usable scope (e.g. {@code ","}),
     * a {@link DataStoreException} is thrown instead of silently returning an empty collection.
     * <p>
     * This is the single resolution a caller must validate against before assuming a scope is
     * granted, since the raw parameter value is neither trimmed nor split.
     *
     * @param params The data store parameters.
     * @return The OAuth scopes.
     */
    protected static Collection<String> resolveScopes(final DataStoreParams params) {
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
                final Drive.Files.List list = getDrive().files().list().setPageToken(pageToken);
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
     * Retrieves every permission of a file or of a shared drive, following pagination.
     * <p>
     * {@code useDomainAdminAccess} is only honoured by the Drive API when {@code fileId} refers to a
     * shared drive and the caller is a domain administrator. Pass {@code false} for an ordinary file ID.
     *
     * @param fileId The file ID or the shared drive ID.
     * @param useDomainAdminAccess Whether to issue the request as a domain administrator.
     * @return The permissions. Never null, but possibly empty.
     */
    public List<Permission> getPermissions(final String fileId, final boolean useDomainAdminAccess) {
        if (logger.isDebugEnabled()) {
            logger.debug("fileId: {}, useDomainAdminAccess: {}", fileId, useDomainAdminAccess);
        }
        final List<Permission> permissionList = new ArrayList<>();
        String pageToken = null;
        try {
            do {
                final Drive.Permissions.List list = getDrive().permissions()
                        .list(fileId)
                        .setSupportsAllDrives(Boolean.TRUE)
                        .setUseDomainAdminAccess(Boolean.valueOf(useDomainAdminAccess))
                        .setPageSize(Integer.valueOf(PERMISSION_PAGE_SIZE_LIMIT))
                        .setFields(PERMISSION_FIELDS)
                        .setPageToken(pageToken);
                final PermissionList result = list.execute();
                if (result.getPermissions() != null) {
                    permissionList.addAll(result.getPermissions());
                }
                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        } catch (final IOException e) {
            throw new DataStoreException("Failed to access permissions of " + fileId + ".", e);
        }
        return permissionList;
    }

    /**
     * Enumerates every shared drive of the domain, following pagination.
     * <p>
     * Requires the caller to be impersonating a Google Workspace domain administrator: the request
     * sets {@code useDomainAdminAccess=true}, which returns every shared drive of the domain the
     * requester administers, whether or not the requester is a member of it.
     *
     * @param consumer A consumer for each shared drive.
     */
    public void getDrives(final Consumer<com.google.api.services.drive.model.Drive> consumer) {
        String pageToken = null;
        try {
            do {
                final Drive.Drives.List list = getDrive().drives()
                        .list()
                        .setUseDomainAdminAccess(Boolean.TRUE)
                        .setPageSize(Integer.valueOf(DRIVE_PAGE_SIZE_LIMIT))
                        .setFields(DRIVE_FIELDS)
                        .setPageToken(pageToken);
                final DriveList result = list.execute();
                if (result.getDrives() != null) {
                    for (final com.google.api.services.drive.model.Drive sharedDrive : result.getDrives()) {
                        consumer.accept(sharedDrive);
                    }
                }
                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        } catch (final IOException e) {
            throw new DataStoreException("Failed to access shared drives.", e);
        }
    }

    /**
     * Walks every file of one shared drive, following pagination.
     * <p>
     * Unlike {@link #getFiles(String, String, String, String, Consumer)} with
     * {@link #ALL_DRIVES}, this scopes the listing to a single {@code driveId}, so a crawl can walk
     * the drives of a domain one at a time.
     *
     * @param driveId The shared drive ID.
     * @param q The query to filter files, or null.
     * @param fields The field projection, or null.
     * @param consumer A consumer for each file.
     */
    public void getFilesInDrive(final String driveId, final String q, final String fields, final Consumer<File> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("driveId: {}, query: {}, fields: {}", driveId, q, fields);
        }
        String pageToken = null;
        try {
            do {
                final Drive.Files.List list = getDrive().files()
                        .list()
                        .setCorpora(DRIVE_CORPORA)
                        .setDriveId(driveId)
                        .setIncludeItemsFromAllDrives(Boolean.TRUE)
                        .setSupportsAllDrives(Boolean.TRUE)
                        .setPageToken(pageToken);
                if (StringUtil.isNotBlank(q)) {
                    list.setQ(q);
                }
                if (StringUtil.isNotBlank(fields)) {
                    list.setFields(fields);
                }
                final FileList result = list.execute();
                if (result.getFiles() != null) {
                    for (final File file : result.getFiles()) {
                        consumer.accept(file);
                    }
                }
                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        } catch (final IOException e) {
            throw new DataStoreException("Failed to access files in a shared drive: " + driveId, e);
        }
    }

    /**
     * Lists the primary email address of every user of the domain through the Admin SDK Directory API.
     * <p>
     * Implemented as a plain REST call so that {@code google-api-services-admin-directory} does not
     * have to become a dependency: {@code users.list} is a single GET. The response is parsed with
     * the same {@link GsonFactory} the Drive service already uses, so no second JSON stack is pulled in.
     * <p>
     * The caller must be impersonating an administrator and the credentials must carry
     * {@link #ADMIN_DIRECTORY_USER_READONLY_SCOPE}, which is not part of {@link #DEFAULT_SCOPES}.
     *
     * @param userQuery The Admin SDK {@code query} used to narrow the users down, or null.
     * @return The primary email addresses. Never null, but possibly empty.
     */
    public List<String> listUsers(final String userQuery) {
        if (logger.isDebugEnabled()) {
            logger.debug("userQuery: {}", userQuery);
        }
        final List<String> userList = new ArrayList<>();
        final HttpRequestFactory requestFactory = createAdminRequestFactory();
        final JsonObjectParser parser = new JsonObjectParser(GsonFactory.getDefaultInstance());
        String pageToken = null;
        try {
            do {
                final GenericUrl url = new GenericUrl(ADMIN_DIRECTORY_USERS_URL);
                url.put("customer", ADMIN_CUSTOMER);
                url.put("maxResults", Integer.toString(ADMIN_MAX_RESULTS));
                url.put("projection", "basic");
                url.put("fields", ADMIN_USER_FIELDS);
                if (StringUtil.isNotBlank(userQuery)) {
                    url.put("query", userQuery);
                }
                if (pageToken != null) {
                    url.put("pageToken", pageToken);
                }
                final HttpResponse response = requestFactory.buildGetRequest(url).setParser(parser).execute();
                try {
                    final GenericJson json = response.parseAs(GenericJson.class);
                    pageToken = json == null ? null : collectUserEmails(json, userList);
                } finally {
                    response.disconnect();
                }
            } while (pageToken != null);
        } catch (final IOException e) {
            throw new DataStoreException("Failed to list users of the domain.", e);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("users: {}", userList.size());
        }
        return userList;
    }

    /**
     * Appends the primary email address of every user of one {@code users.list} page to the given list.
     * <p>
     * The page is parsed untyped, so a user without {@code primaryEmail} is skipped rather than
     * added as a null entry.
     *
     * @param json One page of the {@code users.list} response.
     * @param userList The list to append the email addresses to.
     * @return The token of the next page, or null if this was the last page.
     */
    protected static String collectUserEmails(final GenericJson json, final List<String> userList) {
        if (json.get("users") instanceof final List<?> users) {
            for (final Object user : users) {
                if (user instanceof final Map<?, ?> userMap) {
                    final Object email = userMap.get("primaryEmail");
                    if (email != null) {
                        userList.add(email.toString());
                    }
                }
            }
        }
        final Object nextPageToken = json.get("nextPageToken");
        return nextPageToken != null ? nextPageToken.toString() : null;
    }

    /**
     * Creates the request factory used for Admin SDK calls.
     * <p>
     * Reuses {@link #requestInitializer}, so an Admin SDK request carries the same credentials and
     * the same read and connect timeouts as a Drive request. Exists as an override point for tests.
     *
     * @return The request factory.
     */
    protected HttpRequestFactory createAdminRequestFactory() {
        return httpTransport.createRequestFactory(requestInitializer);
    }

    /**
     * Returns a client that acts as another user through domain-wide delegation.
     * <p>
     * The returned client re-runs the ordinary construction path on a copy of this client's
     * parameters with {@link #IMPERSONATE_USER} overridden, so it gets its own credentials, its own
     * request initializer and its own Drive service, all bound to {@code userEmail}. Nothing that
     * binds a request to a particular user is shared, so several per-user clients can be alive at
     * once without interfering. This client is left untouched, including its own impersonation.
     * <p>
     * Only {@link #httpTransport} is shared, as a connection factory carrying no per-user state. The
     * returned client therefore does not own the transport and must not outlive this client.
     *
     * @param userEmail The email address of the user to act as.
     * @return A client bound to that user.
     */
    public GSuiteClient forUser(final String userEmail) {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating a client for {}", userEmail);
        }
        final DataStoreParams userParams = params.newInstance();
        userParams.put(IMPERSONATE_USER, userEmail);
        final GSuiteClient client = new GSuiteClient(userParams, httpTransport);
        client.setApplicationName(applicationName);
        return client;
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

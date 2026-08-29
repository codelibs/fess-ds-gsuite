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
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.util.TemporaryFileInputStream;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport.Builder;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.About;
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

    /** Corpora value that scopes files.list to the files of the requesting user, i.e. their My Drive. */
    public static final String USER_CORPORA = "user";

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

    /** Parameter key for the page size of files.list. */
    protected static final String PAGE_SIZE = "page_size";

    /** Default page size of files.list, which is also the maximum the API accepts. */
    protected static final int DEFAULT_PAGE_SIZE = 1000;

    /** The maximum page size accepted by files.list. A larger value is rejected with a 400. */
    protected static final int FILE_PAGE_SIZE_LIMIT = 1000;

    /** Parameter key for the page size of permissions.list and drives.list. */
    protected static final String PERMISSION_PAGE_SIZE = "permission_page_size";

    /** Default page size of permissions.list and drives.list, which is also the maximum both accept. */
    protected static final int DEFAULT_PERMISSION_PAGE_SIZE = 100;

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

    /** Parameter key for the maximum number of retries of a throttled Drive API call. */
    protected static final String MAX_RETRIES = "max_retries";
    /** Parameter key for the initial wait of the exponential back-off, in milliseconds. */
    protected static final String RETRY_INITIAL_INTERVAL_MS = "retry_initial_interval_ms";
    /** Parameter key for the upper bound of the exponential back-off, in milliseconds. */
    protected static final String MAX_BACKOFF_MS = "max_backoff_ms";

    /** Default number of retries. */
    protected static final int DEFAULT_MAX_RETRIES = 5;
    /** Default initial wait of the exponential back-off, in milliseconds. */
    protected static final int DEFAULT_RETRY_INITIAL_INTERVAL_MS = 1000;
    /** Default upper bound of the exponential back-off, in milliseconds. */
    protected static final int DEFAULT_MAX_BACKOFF_MS = 32000;
    /** Upper bound of the additive jitter recommended by Google, in milliseconds. */
    protected static final int MAX_JITTER_MS = 1000;

    /** The output size limit of files.export, documented by Google as 10 MB. */
    protected static final long EXPORT_SIZE_LIMIT = 10L * 1024 * 1024;

    /**
     * The 403 error reasons that mean "slow down" rather than "you may not do this".
     * A 403 with any other reason is a permanent authorization failure and is never retried.
     */
    protected static final Set<String> RETRYABLE_403_REASONS = Set.of("userRateLimitExceeded", "rateLimitExceeded");

    /**
     * The export targets assumed when {@code about.get} cannot be read.
     * <p>
     * These are the conversions Drive v3 has always offered, so falling back to them keeps every
     * native Google document indexed with its content. An empty map would instead drop the content
     * of every Doc, Sheet and Slide of the domain without a single error per file, and failing hard
     * would abort the whole crawl over one metadata call. Google Forms and Google Sites are absent
     * on purpose: they have no export format at all and are indexed with metadata only.
     * </p>
     */
    protected static final Map<String, List<String>> FALLBACK_EXPORT_FORMATS = Map.of(//
            "application/vnd.google-apps.document", List.of("text/plain"), //
            "application/vnd.google-apps.spreadsheet", List.of("text/csv"), //
            "application/vnd.google-apps.presentation", List.of("text/plain"), //
            "application/vnd.google-apps.drawing", List.of("image/png"), //
            "application/vnd.google-apps.script", List.of("application/vnd.google-apps.script+json"));

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

    /** The maximum number of retries of a throttled Drive API call. */
    protected int maxRetries = DEFAULT_MAX_RETRIES;
    /** The initial wait of the exponential back-off, in milliseconds. */
    protected int retryInitialIntervalMillis = DEFAULT_RETRY_INITIAL_INTERVAL_MS;
    /** The upper bound of the exponential back-off, in milliseconds. */
    protected int maxBackOffMillis = DEFAULT_MAX_BACKOFF_MS;

    /** The page size of files.list. */
    protected int pageSize = DEFAULT_PAGE_SIZE;

    /** The page size of permissions.list and drives.list. */
    protected int permissionPageSize = DEFAULT_PERMISSION_PAGE_SIZE;

    /** The cached source mime type to export target map returned by about.get. */
    protected volatile Map<String, List<String>> exportFormats;

    /**
     * Invoked when a listing fails permanently. The default only logs; the data store replaces it
     * with one that also records the failure so that the operator finds it in the failure URL list.
     */
    protected BiConsumer<String, Exception> failureHandler =
            (target, e) -> logger.warn("Failed to access {}. Skipping it and continuing the crawl.", target, e);

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
        pageSize = clampPageSize(PAGE_SIZE, getIntParam(params, PAGE_SIZE, DEFAULT_PAGE_SIZE), FILE_PAGE_SIZE_LIMIT);
        // One value feeds two endpoints, so the smaller of the two caps applies.
        permissionPageSize = clampPageSize(PERMISSION_PAGE_SIZE, getIntParam(params, PERMISSION_PAGE_SIZE, DEFAULT_PERMISSION_PAGE_SIZE),
                Math.min(PERMISSION_PAGE_SIZE_LIMIT, DRIVE_PAGE_SIZE_LIMIT));
        maxRetries = getIntParam(params, MAX_RETRIES, DEFAULT_MAX_RETRIES);
        retryInitialIntervalMillis = getIntParam(params, RETRY_INITIAL_INTERVAL_MS, DEFAULT_RETRY_INITIAL_INTERVAL_MS);
        maxBackOffMillis = getIntParam(params, MAX_BACKOFF_MS, DEFAULT_MAX_BACKOFF_MS);
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
     * <p>
     * A permanent failure in the middle of the paging is handed to {@link #failureHandler} and this
     * method returns: the enclosing loop over the shared drives or the users of the domain must keep
     * going, so that one inaccessible scope does not abort the whole crawl. The pages already
     * consumed are kept, and the failure is reported rather than swallowed.
     * </p>
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
        final String target = describeTarget("files.list(corpora=" + corpora + ", q=" + q + ")");
        long counter = 1;
        String pageToken = null;
        try {
            do {
                final String currentToken = pageToken;
                if (logger.isDebugEnabled()) {
                    logger.debug("Accessing files: {}=>{}", counter, currentToken);
                }
                final FileList result = executeWithRetry(target, () -> {
                    final Drive.Files.List list =
                            getDrive().files().list().setPageToken(currentToken).setPageSize(Integer.valueOf(pageSize));
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
                    return list.execute();
                });
                if (logger.isDebugEnabled()) {
                    logger.debug("filelist: {}", result);
                }
                if (Boolean.TRUE.equals(result.getIncompleteSearch())) {
                    onIncompleteSearch(target);
                }
                if (result.getFiles() != null) {
                    for (final File file : result.getFiles()) {
                        consumer.accept(file);
                    }
                }
                pageToken = result.getNextPageToken();
                counter++;
            } while (pageToken != null);
        } catch (final IOException e) {
            // The token of the next page died with the failed page, so this listing cannot resume.
            reportFailure(target, e);
        }
    }

    /**
     * Describes a listing for the log and for the failure report, naming the identity the client acts
     * as. A crawl runs one client per user, so without the identity a failure cannot be attributed
     * to the user whose Drive could not be listed.
     *
     * @param operation The operation, e.g. {@code files.list(...)}.
     * @return The description.
     */
    protected String describeTarget(final String operation) {
        final String impersonateUser = params.getAsString(IMPERSONATE_USER);
        return StringUtil.isBlank(impersonateUser) ? operation : operation + " as " + impersonateUser;
    }

    /**
     * Sets the handler invoked when a listing fails permanently. The crawl continues after the
     * handler returns, so the handler is the only place the failure is reported.
     *
     * @param failureHandler A handler that receives the description of the failed listing and the
     *            failure. Ignored when null.
     */
    public void setFailureHandler(final BiConsumer<String, Exception> failureHandler) {
        if (failureHandler != null) {
            this.failureHandler = failureHandler;
        }
    }

    /**
     * Hands a permanent listing failure to {@link #failureHandler}.
     * <p>
     * An interruption is never downgraded into a per-target failure: {@link #executeWithRetry} wraps
     * one while waiting to retry, and a stop request must stop the crawl rather than merely skip the
     * scope it happened to interrupt.
     * </p>
     *
     * @param target The description of the failed listing.
     * @param e The failure.
     */
    protected void reportFailure(final String target, final IOException e) {
        if (e.getCause() instanceof final InterruptedException ie) {
            throw new InterruptedRuntimeException(ie);
        }
        failureHandler.accept(target, e);
    }

    /**
     * Called when a files.list response is flagged {@code incompleteSearch}, meaning Drive could not
     * search every corpus and the result set is silently short.
     *
     * @param target The description of the listing, naming the corpora and the query.
     */
    protected void onIncompleteSearch(final String target) {
        logger.warn(
                "Drive could not search every corpus for {}, so the result is incomplete " + "and some items are missing from this crawl.",
                target);
    }

    /**
     * Clamps a page size into the range the endpoint accepts.
     * <p>
     * The caps differ per endpoint -- files.list accepts 1000 while permissions.list and drives.list
     * stop at 100 -- and a value above the cap is rejected with a 400, which would fail every page of
     * the listing rather than just the first.
     * </p>
     *
     * @param key The parameter key, used in the warning.
     * @param value The configured value.
     * @param limit The maximum the endpoint accepts.
     * @return The clamped value.
     */
    protected static int clampPageSize(final String key, final int value, final int limit) {
        if (value < 1) {
            logger.warn("{} must be at least 1: {}. Using 1.", key, Integer.valueOf(value));
            return 1;
        }
        if (value > limit) {
            logger.warn("{} is capped at {} by the API: {}. Using {}.", key, Integer.valueOf(limit), Integer.valueOf(value),
                    Integer.valueOf(limit));
            return limit;
        }
        return value;
    }

    /**
     * Retrieves every permission of a file or of a shared drive, following pagination.
     * <p>
     * {@code useDomainAdminAccess} is only honoured by the Drive API when {@code fileId} refers to a
     * shared drive and the caller is a domain administrator. Pass {@code false} for an ordinary file ID.
     * <p>
     * Unlike the file listings, a permanent failure is thrown rather than reported and skipped: the
     * result is an ACL, the resolver caches it per drive, and a list that lost a page would silently
     * strip the roles of every document of that drive. Either every page is returned or nothing is.
     * </p>
     *
     * @param fileId The file ID or the shared drive ID.
     * @param useDomainAdminAccess Whether to issue the request as a domain administrator.
     * @return The permissions of every page. Never null, but possibly empty.
     */
    public List<Permission> getPermissions(final String fileId, final boolean useDomainAdminAccess) {
        if (logger.isDebugEnabled()) {
            logger.debug("fileId: {}, useDomainAdminAccess: {}", fileId, useDomainAdminAccess);
        }
        final List<Permission> permissionList = new ArrayList<>();
        final String target = describeTarget("permissions.list(" + fileId + ")");
        String pageToken = null;
        try {
            do {
                final String currentToken = pageToken;
                final PermissionList result = executeWithRetry(target,
                        () -> getDrive().permissions()
                                .list(fileId)
                                .setSupportsAllDrives(Boolean.TRUE)
                                .setUseDomainAdminAccess(Boolean.valueOf(useDomainAdminAccess))
                                .setPageSize(Integer.valueOf(permissionPageSize))
                                .setFields(PERMISSION_FIELDS)
                                .setPageToken(currentToken)
                                .execute());
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
     * <p>
     * This listing decides the whole scope of a shared drive crawl, so a permanent failure is thrown
     * rather than reported and skipped: a crawl that enumerated no drive has not partially succeeded,
     * and must not be able to report success while indexing nothing.
     * </p>
     *
     * @param consumer A consumer for each shared drive.
     */
    public void getDrives(final Consumer<com.google.api.services.drive.model.Drive> consumer) {
        final String target = describeTarget("drives.list");
        String pageToken = null;
        try {
            do {
                final String currentToken = pageToken;
                final DriveList result = executeWithRetry(target,
                        () -> getDrive().drives()
                                .list()
                                .setUseDomainAdminAccess(Boolean.TRUE)
                                .setPageSize(Integer.valueOf(permissionPageSize))
                                .setFields(DRIVE_FIELDS)
                                .setPageToken(currentToken)
                                .execute());
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
     * <p>
     * Like {@link #getFiles(String, String, String, String, Consumer)}, a permanent failure is
     * reported to {@link #failureHandler} instead of being thrown, so that one shared drive the
     * crawler may not read does not abort the drives that follow it.
     * </p>
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
        final String target = describeTarget("files.list(corpora=" + DRIVE_CORPORA + ", driveId=" + driveId + ", q=" + q + ")");
        String pageToken = null;
        try {
            do {
                final String currentToken = pageToken;
                final FileList result = executeWithRetry(target, () -> {
                    final Drive.Files.List list = getDrive().files()
                            .list()
                            .setCorpora(DRIVE_CORPORA)
                            .setDriveId(driveId)
                            .setIncludeItemsFromAllDrives(Boolean.TRUE)
                            .setSupportsAllDrives(Boolean.TRUE)
                            .setPageSize(Integer.valueOf(pageSize))
                            .setPageToken(currentToken);
                    if (StringUtil.isNotBlank(q)) {
                        list.setQ(q);
                    }
                    if (StringUtil.isNotBlank(fields)) {
                        list.setFields(fields);
                    }
                    return list.execute();
                });
                if (Boolean.TRUE.equals(result.getIncompleteSearch())) {
                    onIncompleteSearch(target);
                }
                if (result.getFiles() != null) {
                    for (final File file : result.getFiles()) {
                        consumer.accept(file);
                    }
                }
                pageToken = result.getNextPageToken();
            } while (pageToken != null);
        } catch (final IOException e) {
            reportFailure(target, e);
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
        final String target = describeTarget("users.list(query=" + userQuery + ")");
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
                final HttpResponse response =
                        executeWithRetry(target, () -> requestFactory.buildGetRequest(url).setParser(parser).execute());
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
     * Only {@link #httpTransport} is shared, as a connection factory carrying no per-user state, and
     * {@link #failureHandler}, so that a failure hit while listing that user's Drive is reported
     * through the same channel as one hit by this client. The returned client therefore does not own
     * the transport and must not outlive this client.
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
        client.setFailureHandler(failureHandler);
        return client;
    }

    /**
     * Returns the live "source mime type to export targets" map from
     * {@code about.get?fields=exportFormats}.
     * <p>
     * The map is fetched once per client and cached, so the crawl spends one metadata call rather
     * than one per file. A failure is not fatal and does not yield an empty map either: it falls
     * back to {@link #FALLBACK_EXPORT_FORMATS} so native Google documents keep their content, and
     * the fallback is cached like a successful answer so a broken about.get is not retried per file.
     * </p>
     *
     * @return The export formats. Never null and never empty.
     */
    public Map<String, List<String>> getExportFormats() {
        final Map<String, List<String>> cached = exportFormats;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (exportFormats != null) {
                return exportFormats;
            }
            Map<String, List<String>> formats = null;
            try {
                final About about = executeWithRetry("about.get", () -> getDrive().about().get().setFields("exportFormats").execute());
                formats = about.getExportFormats();
            } catch (final IOException e) {
                if (e.getCause() instanceof final InterruptedException ie) {
                    // A stop request must stop the crawl, not silently downgrade it to the fallback.
                    throw new InterruptedRuntimeException(ie);
                }
                logger.warn("Failed to read the export formats. Falling back to the conversions Drive has always supported.", e);
            }
            exportFormats = formats == null || formats.isEmpty() ? FALLBACK_EXPORT_FORMATS : formats;
            return exportFormats;
        }
    }

    /**
     * Thrown when an export outgrows {@link #getExportSizeLimit()}. It is a runtime exception
     * because it has to travel out of {@code executeMediaAndDownloadTo}, whose OutputStream contract
     * only allows an IOException.
     */
    protected static class ExportSizeLimitExceededException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new ExportSizeLimitExceededException.
         *
         * @param message The detail message.
         */
        protected ExportSizeLimitExceededException(final String message) {
            super(message);
        }
    }

    /**
     * A {@link ByteArrayOutputStream} that refuses to buffer more than a fixed number of bytes.
     * Without it an oversized export grows the heap without bound, since a native Google file
     * declares no size and the response is streamed straight into memory (D-18).
     */
    protected static class BoundedByteArrayOutputStream extends ByteArrayOutputStream {

        /** The maximum number of bytes that may be buffered. */
        protected final long limit;

        /**
         * Constructs a new BoundedByteArrayOutputStream.
         *
         * @param limit The maximum number of bytes that may be buffered.
         */
        protected BoundedByteArrayOutputStream(final long limit) {
            this.limit = limit;
        }

        @Override
        public synchronized void write(final int b) {
            checkLimit(1L);
            super.write(b);
        }

        @Override
        public synchronized void write(final byte[] b, final int off, final int len) {
            checkLimit(len);
            super.write(b, off, len);
        }

        /**
         * Fails before buffering when the write would push the buffer past the limit.
         *
         * @param additional The number of bytes about to be written.
         */
        protected void checkLimit(final long additional) {
            if (size() + additional > limit) {
                throw new ExportSizeLimitExceededException("The exported content exceeds " + limit + " bytes.");
            }
        }
    }

    /**
     * Returns the maximum number of bytes an export may produce.
     *
     * @return The limit, in bytes.
     */
    protected long getExportSizeLimit() {
        return EXPORT_SIZE_LIMIT;
    }

    /**
     * Extracts the text from a file through {@code files.export}.
     * <p>
     * Note that {@code files.export} has no {@code supportsAllDrives} parameter in the Drive v3
     * API, so unlike {@code files.get} this request cannot opt into shared drive support.
     * <p>
     * A native Google file declares no size, so {@link #getExportSizeLimit()} is the only bound on
     * the buffer. An export that outgrows it is abandoned before the heap is spent and the file is
     * skipped with a warning: a {@link MaxLengthExceededException} is a
     * {@code CrawlingAccessException}, so the data store records the file in the admin failure list
     * exactly as it already does for a file over {@code max_size}. Returning an empty string instead
     * would index a document indistinguishable from one that genuinely has no text (D-18).
     * </p>
     * @param id The ID of the file.
     * @param mimeType The mime type of the file.
     * @return The text of the file.
     */
    public String extractFileText(final String id, final String mimeType) {
        final long limit = getExportSizeLimit();
        try (BoundedByteArrayOutputStream out = new BoundedByteArrayOutputStream(limit)) {
            executeWithRetry("files.export(" + id + ")", () -> {
                getDrive().files().export(id, mimeType).executeMediaAndDownloadTo(out);
                return Boolean.TRUE;
            });
            return out.toString(Constants.UTF_8);
        } catch (final ExportSizeLimitExceededException e) {
            logger.warn("Skipped the content of {}: the {} export is larger than the {} byte limit of files.export.", id, mimeType,
                    Long.valueOf(limit));
            throw new MaxLengthExceededException(
                    "The " + mimeType + " export of " + id + " is larger than the " + limit + " byte limit of files.export.");
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
            // A retry only ever happens on a status the request failed with, which the client raises
            // before a single byte is written, so the buffer never accumulates two attempts.
            executeWithRetry("files.get(" + id + ")", () -> {
                newFileGetRequest(id).executeMediaAndDownloadTo(dfos);
                return Boolean.TRUE;
            });
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
     * Reads an integer parameter, falling back to a default when it is blank or malformed.
     *
     * @param params The data store parameters.
     * @param key The parameter key.
     * @param defaultValue The value used when the parameter is absent or not a number.
     * @return The parameter value.
     */
    protected static int getIntParam(final DataStoreParams params, final String key, final int defaultValue) {
        final String value = params.getAsString(key);
        if (StringUtil.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (final NumberFormatException e) {
            logger.warn("Invalid {}: {}. Using {}.", key, value, Integer.valueOf(defaultValue));
            return defaultValue;
        }
    }

    /**
     * An exponential back-off that follows the wait interval Google documents for the Drive API:
     * {@code min((2^n) * initialInterval + random(0, 1000), maxBackOff)}.
     * The jitter is additive, unlike {@code com.google.api.client.util.ExponentialBackOff}, whose
     * jitter is multiplicative.
     */
    protected static class GoogleBackOff implements BackOff {

        /** The maximum number of retries before {@link BackOff#STOP} is returned. */
        protected final int maxRetries;
        /** The initial wait, in milliseconds. */
        protected final long initialIntervalMillis;
        /** The upper bound of a single wait, in milliseconds. */
        protected final long maxBackOffMillis;
        /** The source of the additive jitter. */
        protected final Random random;
        /** The number of retries already handed out. */
        protected int retryCount;

        /**
         * Constructs a new GoogleBackOff.
         *
         * @param maxRetries The maximum number of retries.
         * @param initialIntervalMillis The initial wait, in milliseconds.
         * @param maxBackOffMillis The upper bound of a single wait, in milliseconds.
         * @param random The source of the additive jitter.
         */
        protected GoogleBackOff(final int maxRetries, final long initialIntervalMillis, final long maxBackOffMillis, final Random random) {
            this.maxRetries = maxRetries;
            this.initialIntervalMillis = initialIntervalMillis;
            this.maxBackOffMillis = maxBackOffMillis;
            this.random = random;
        }

        @Override
        public void reset() {
            retryCount = 0;
        }

        @Override
        public long nextBackOffMillis() {
            if (retryCount >= maxRetries) {
                return BackOff.STOP;
            }
            // (2^n) * initialInterval; the shift is capped so a misconfigured max_retries cannot overflow.
            final long exponential = initialIntervalMillis << Math.min(retryCount, 32);
            retryCount++;
            return Math.min(exponential + random.nextInt(MAX_JITTER_MS), maxBackOffMillis);
        }

        /**
         * Returns the upper bound of a single wait, in milliseconds.
         *
         * @return The upper bound, in milliseconds.
         */
        protected long getMaxBackOffMillis() {
            return maxBackOffMillis;
        }

        /**
         * Returns the number of retries already handed out.
         *
         * @return The retry count.
         */
        protected int getRetryCount() {
            return retryCount;
        }
    }

    /**
     * A Drive API call that may fail with an {@link IOException}.
     *
     * @param <T> The result type.
     */
    @FunctionalInterface
    protected interface DriveCall<T> {
        /**
         * Executes the call.
         *
         * @return The result of the call.
         * @throws IOException If the call fails.
         */
        T call() throws IOException;
    }

    /**
     * Creates a back-off configured from the data store parameters.
     *
     * @return A new back-off.
     */
    protected GoogleBackOff newBackOff() {
        return new GoogleBackOff(maxRetries, retryInitialIntervalMillis, maxBackOffMillis, new Random());
    }

    /**
     * Decides whether a failed Drive API call is worth retrying.
     * <p>
     * A 403 is ambiguous: Drive returns it both for quota exhaustion and for permanent authorization
     * failures such as a missing scope. Only the JSON body distinguishes them, and
     * {@link HttpResponseException#getContent()} already holds that body as a string, so the
     * classification is done here rather than in an {@code HttpUnsuccessfulResponseHandler}, whose
     * {@code handleResponse} sees the body as a one-shot stream.
     * </p>
     *
     * @param e The failure.
     * @return true if the call should be retried.
     */
    protected static boolean isRetryable(final HttpResponseException e) {
        final int statusCode = e.getStatusCode();
        if (statusCode == 429 || statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504) {
            return true;
        }
        if (statusCode != 403) {
            return false;
        }
        if (e instanceof final GoogleJsonResponseException je && je.getDetails() != null && je.getDetails().getErrors() != null) {
            for (final GoogleJsonError.ErrorInfo info : je.getDetails().getErrors()) {
                if (RETRYABLE_403_REASONS.contains(info.getReason())) {
                    return true;
                }
            }
            return false;
        }
        // A call issued through a plain HttpRequest, such as the Admin SDK one, never becomes a
        // GoogleJsonResponseException, so the reason has to be read out of the raw body. The quotes
        // keep a longer reason that merely ends with a retryable one, e.g. sharingRateLimitExceeded,
        // from being mistaken for a quota error.
        final String content = e.getContent();
        if (content == null) {
            return false;
        }
        for (final String reason : RETRYABLE_403_REASONS) {
            if (content.contains("\"" + reason + "\"")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the wait requested by a {@code Retry-After} header, in milliseconds.
     * Only the delta-seconds form is honoured; an HTTP-date falls back to the exponential wait.
     *
     * @param e The failure.
     * @return The requested wait in milliseconds, or 0 when there is none.
     */
    protected static long getRetryAfterMillis(final HttpResponseException e) {
        final HttpHeaders headers = e.getHeaders();
        if (headers == null) {
            return 0L;
        }
        final String retryAfter = headers.getRetryAfter();
        if (StringUtil.isBlank(retryAfter)) {
            return 0L;
        }
        try {
            return Long.parseLong(retryAfter.trim()) * 1000L;
        } catch (final NumberFormatException ex) {
            return 0L;
        }
    }

    /**
     * Returns how long a retryable failure should be waited out, in milliseconds.
     * <p>
     * A {@code Retry-After} wins over the exponential wait, but is clamped by the maximum back-off so
     * that a hostile or mistaken header cannot stall the crawl for hours. The result is never
     * negative, so a misconfigured interval cannot turn into an {@link IllegalArgumentException} at
     * the {@link Thread#sleep(long)} call.
     * </p>
     *
     * @param backOffMillis The exponential wait handed out by the back-off.
     * @param e The failure.
     * @param maxBackOffMillis The upper bound of a single wait, in milliseconds.
     * @return The wait, in milliseconds.
     */
    protected static long computeWaitMillis(final long backOffMillis, final HttpResponseException e, final long maxBackOffMillis) {
        final long waitMillis = Math.min(Math.max(backOffMillis, getRetryAfterMillis(e)), maxBackOffMillis);
        return waitMillis > 0L ? waitMillis : 0L;
    }

    /**
     * Executes a Drive API call, retrying throttled and transient failures with an exponential back-off.
     *
     * @param <T> The result type.
     * @param operation A short description used in the log messages.
     * @param backOff The back-off that bounds the retries.
     * @param call The call to execute.
     * @return The result of the call.
     * @throws IOException If the call fails permanently or the retry budget is exhausted.
     */
    protected static <T> T executeWithRetry(final String operation, final GoogleBackOff backOff, final DriveCall<T> call)
            throws IOException {
        while (true) {
            try {
                return call.call();
            } catch (final HttpResponseException e) {
                if (!isRetryable(e)) {
                    throw e;
                }
                final long backOffMillis = backOff.nextBackOffMillis();
                if (backOffMillis == BackOff.STOP) {
                    logger.warn("Giving up {} after {} retries. (status: {})", operation, Integer.valueOf(backOff.getRetryCount()),
                            Integer.valueOf(e.getStatusCode()));
                    throw e;
                }
                final long waitMillis = computeWaitMillis(backOffMillis, e, backOff.getMaxBackOffMillis());
                logger.warn("Retrying {} in {} ms. (status: {})", operation, Long.valueOf(waitMillis), Integer.valueOf(e.getStatusCode()));
                try {
                    Thread.sleep(waitMillis);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting to retry " + operation, ie);
                }
            }
        }
    }

    /**
     * Executes a Drive API call with a back-off configured from the data store parameters.
     *
     * @param <T> The result type.
     * @param operation A short description used in the log messages.
     * @param call The call to execute.
     * @return The result of the call.
     * @throws IOException If the call fails permanently or the retry budget is exhausted.
     */
    protected <T> T executeWithRetry(final String operation, final DriveCall<T> call) throws IOException {
        return executeWithRetry(operation, newBackOff(), call);
    }

    /**
     * Sets the application name.
     * @param applicationName The application name.
     */
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }
}

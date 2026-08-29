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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.File;

/**
 * DataStore for Google Drive.
 */
public class GoogleDriveDataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(GoogleDriveDataStore.class);

    /** Default maximum size of a file to be indexed. */
    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    /** Default thread pool termination timeout in seconds. */
    protected static final long DEFAULT_THREAD_POOL_TIMEOUT_SECONDS = 60L;

    /** Mime type prefix shared by every native Google type, none of which can be downloaded with alt=media. */
    protected static final String GOOGLE_APPS_MIMETYPE_PREFIX = "application/vnd.google-apps.";

    /**
     * The export targets preferred for each native Google type, most preferred first. Only a target
     * that actually appears in the live exportFormats map is used, so this table never forces an
     * unsupported conversion.
     */
    protected static final Map<String, List<String>> PREFERRED_EXPORT_MIMETYPES = Map.of(//
            GOOGLE_APPS_MIMETYPE_PREFIX + "document", List.of("text/markdown", "text/plain", "text/html"), //
            GOOGLE_APPS_MIMETYPE_PREFIX + "spreadsheet", List.of("text/tab-separated-values", "text/csv"), //
            GOOGLE_APPS_MIMETYPE_PREFIX + "presentation", List.of("text/plain"), //
            GOOGLE_APPS_MIMETYPE_PREFIX + "drawing", List.of("image/png"), //
            GOOGLE_APPS_MIMETYPE_PREFIX + "script", List.of("application/vnd.google-apps.script+json"));

    /** The Apps Script export target, whose payload is a JSON bundle rather than text. */
    protected static final String SCRIPT_EXPORT_MIMETYPE = "application/vnd.google-apps.script+json";

    // parameters
    /** Parameter key for the maximum file size. */
    protected static final String MAX_SIZE = "max_size";
    /** Parameter key for ignoring folders. */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Parameter key for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Parameter key for supported mime types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Parameter key for include patterns. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Parameter key for exclude patterns. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Parameter key for the URL filter. */
    protected static final String URL_FILTER = "url_filter";
    /** Parameter key for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter key for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter key for the thread pool termination timeout in seconds. */
    protected static final String THREAD_POOL_TIMEOUT_SECONDS = "thread_pool_timeout_seconds";
    /**
     * Parameter key for the role format applied to a domain-wide permission. The value is
     * {@code {domain}}-substituted and then passed through
     * {@link org.codelibs.fess.helper.PermissionHelper#encode(String)}, so it accepts the same
     * {@code {user}}/{@code {group}}/{@code {role}} input notation as {@link #DEFAULT_PERMISSIONS}.
     */
    protected static final String DOMAIN_PERMISSION_FORMAT = "domain_permission_format";

    /**
     * Default role format for a domain-wide permission. {@code {domain}} is replaced with the
     * domain name and the result is encoded via
     * {@link org.codelibs.fess.helper.PermissionHelper#encode(String)}, so {@code {group}} here
     * is the same INPUT notation {@code encode} accepts for {@link #DEFAULT_PERMISSIONS}, not a
     * literal string that reaches the index.
     */
    protected static final String DEFAULT_DOMAIN_PERMISSION_FORMAT = "{group}{domain}";

    /**
     * Config map key holding one {@link DrivePermissionResolver} per {@link GSuiteClient}. The
     * value is a map, not a single resolver, because the resolver caches shared drive ACLs and a
     * crawl may run several clients.
     */
    protected static final String PERMISSION_RESOLVERS = "permission_resolvers";

    /** Parameter key that enables incremental crawling. */
    protected static final String INCREMENTAL = "incremental";

    /**
     * The parameter {@code DataIndexHelper} reads to skip its stale document sweep. Its own constant
     * is private, so the key is spelled out here.
     */
    protected static final String DELETE_OLD_DOCS = "delete_old_docs";

    /** Parameter key for the crawl target. */
    protected static final String CRAWL_TARGET = "crawl_target";
    /** Parameter key for the administrator account to impersonate. */
    protected static final String IMPERSONATE_USER = "impersonate_user";
    /** Parameter key for the Admin SDK user query. */
    protected static final String USER_QUERY = "user_query";

    /** Crawl target that keeps the pre-15.9 behaviour of the service account's own view. */
    protected static final String TARGET_LEGACY = "legacy";
    /** Crawl target that walks every shared drive of the domain. */
    protected static final String TARGET_SHARED_DRIVES = "shared_drives";
    /** Crawl target that walks the My Drive of every directory user. */
    protected static final String TARGET_USERS = "users";
    /** Crawl target that walks both shared drives and every user's My Drive. */
    protected static final String TARGET_BOTH = "both";

    /**
     * Parameter keys that carry credentials and must never reach the script evaluation context,
     * since a script value can be indexed and read back by anyone with search access. This is the
     * single place to add a key if a later phase introduces another secret parameter.
     * <p>
     * {@code proxy_username} is stripped alongside the password for the same reason
     * {@code client_email} is: it is not a secret in itself, but it names an internal account and is
     * half of a credential pair, which is not something a search result should disclose.
     * </p>
     */
    protected static final String[] SECRET_PARAM_KEYS = { GSuiteClient.PRIVATE_KEY_PARAM, GSuiteClient.PRIVATE_KEY_ID_PARAM,
            GSuiteClient.CLIENT_EMAIL_PARAM, GSuiteClient.PROXY_USERNAME, GSuiteClient.PROXY_PASSWORD };

    // scripts
    /** Script key for the file object. */
    protected static final String FILE = "file";
    /** Script key for the file name. */
    protected static final String FILE_NAME = "name";
    /** Script key for the file description. */
    protected static final String FILE_DESCRIPTION = "description";
    /** Script key for the file contents. */
    protected static final String FILE_CONTENTS = "contents";
    /** Script key for the file mime type. */
    protected static final String FILE_MIMETYPE = "mimetype";
    /** Script key for the file type. */
    protected static final String FILE_FILETYPE = "filetype";
    /** Script key for the file thumbnail link. */
    protected static final String FILE_THUMBNAIL_LINK = "thumbnail_link";
    /** Script key for the file web view link. */
    protected static final String FILE_WEB_VIEW_LINK = "web_view_link";
    /** Script key for the file web content link. */
    protected static final String FILE_WEB_CONTENT_LINK = "web_content_link";
    /** Script key for the file created time. */
    protected static final String FILE_CREATED_TIME = "created_time";
    /** Script key for the file modified time. */
    protected static final String FILE_MODIFIED_TIME = "modified_time";
    /** Script key for whether writers can share the file. */
    protected static final String FILE_WRITERS_CAN_SHARE = "writers_can_share";
    /** Script key for whether viewers can copy content. */
    protected static final String FILE_VIEWERS_CAN_COPY_CONTENT = "viewers_can_copy_content";
    /** Script key for the time the file was viewed by the user. */
    protected static final String FILE_VIEWED_BY_ME_TIME = "viewed_by_me_time";
    /** Script key for whether the file was viewed by the user. */
    protected static final String FILE_VIEWED_BY_ME = "viewed_by_me";
    /** Script key for video media metadata. */
    protected static final String FILE_VIDEO_MEDIA_METADATA = "video_media_metadata";
    /** Script key for the file version. */
    protected static final String FILE_VERSION = "version";
    /** Script key for the trashing user. */
    protected static final String FILE_TRASHING_USER = "trashing_user";
    /** Script key for the trashed time. */
    protected static final String FILE_TRASHED_TIME = "trashed_time";
    /** Script key for whether the file is trashed. */
    protected static final String FILE_TRASHED = "trashed";
    /** Script key for the thumbnail version. */
    protected static final String FILE_THUMBNAIL_VERSION = "thumbnail_version";
    /** Script key for the team drive ID. */
    protected static final String FILE_TEAM_DRIVE_ID = "team_drive_id";
    /** Script key for whether the file is shared. */
    protected static final String FILE_SHARED = "shared";
    /** Script key for the quota bytes used. */
    protected static final String FILE_QUOTA_BYTES_USED = "quota_bytes_used";
    /** Script key for the file parents. */
    protected static final String FILE_PARENTS = "parents";
    /** Script key for the file owners. */
    protected static final String FILE_OWNERS = "owners";
    /** Script key for whether the file is owned by the user. */
    protected static final String FILE_OWNED_BY_ME = "owned_by_me";
    /** Script key for the original file name. */
    protected static final String FILE_ORIGINAL_FILENAME = "original_filename";
    /** Script key for the time the file was modified by the user. */
    protected static final String FILE_MODIFIED_BY_ME_TIME = "modified_by_me_time";
    /** Script key for whether the file was modified by the user. */
    protected static final String FILE_MODIFIED_BY_ME = "modified_by_me";
    /** Script key for the MD5 checksum. */
    protected static final String FILE_MD5_CHECKSUM = "md5_checksum";
    /** Script key for the last modifying user. */
    protected static final String FILE_LAST_MODIFYING_USER = "last_modifying_user";
    /** Script key for the file kind. */
    protected static final String FILE_KIND = "kind";
    /** Script key for whether the app is authorized. */
    protected static final String FILE_IS_APP_AUTHORIZED = "is_app_authorized";
    /** Script key for image media metadata. */
    protected static final String FILE_IMAGE_MEDIA_METADATA = "image_media_metadata";
    /** Script key for the file ID. */
    protected static final String FILE_ID = "id";
    /** Script key for the file icon link. */
    protected static final String FILE_ICON_LINK = "icon_link";
    /** Script key for the head revision ID. */
    protected static final String FILE_HEAD_REVISION_ID = "head_revision_id";
    /** Script key for whether the file has a thumbnail. */
    protected static final String FILE_HAS_THUMBNAIL = "has_thumbnail";
    /** Script key for whether the file has augmented permissions. */
    protected static final String FILE_HAS_AUGMENTED_PERMISSIONS = "has_augmented_permissions";
    /** Script key for the full file extension. */
    protected static final String FILE_FULL_FILE_EXTENSION = "full_file_extension";
    /** Script key for the folder color RGB. */
    protected static final String FILE_FOLDER_COLOR_RGB = "folder_color_rgb";
    /** Script key for the file extension. */
    protected static final String FILE_FILE_EXTENSION = "file_extension";
    /** Script key for the export links. */
    protected static final String FILE_EXPORT_LINKS = "export_links";
    /** Script key for whether the file is explicitly trashed. */
    protected static final String FILE_EXPLICITLY_TRASHED = "explicitly_trashed";
    /** Script key for whether copying requires writer permission. */
    protected static final String FILE_COPY_REQUIRES_WRITER_PERMISSION = "copy_requires_writer_permission";
    /** Script key for the app properties. */
    protected static final String FILE_APP_PROPERTIES = "app_properties";
    /** Script key for the file capabilities. */
    protected static final String FILE_CAPABILITIES = "capabilities";
    /** Script key for the content hints. */
    protected static final String FILE_CONTENT_HINTS = "content_hints";
    /** Script key for the class info. */
    protected static final String FILE_CLASS_INFO = "class_info";
    /** Script key for the file URL. */
    protected static final String FILE_URL = "url";
    /** Script key for the file size. */
    protected static final String FILE_SIZE = "size";
    /** Script key for the file roles. */
    protected static final String FILE_ROLES = "roles";

    /** The name of the extractor to use. */
    protected String extractorName = "tikaExtractor";

    // other
    /** Parameter key for the file field projection. */
    protected static final String FIELDS = "fields";

    /** The wildcard projection. Accepted but expensive: files.list then returns every field of every file. */
    protected static final String WILDCARD_FIELDS = "*";

    /**
     * The default {@code files(...)} projection. It covers every getter {@code buildFileMap} reads plus
     * the fields the ACL resolution, the index URL and the incremental crawl need. A field that is not
     * listed here is null in the script context, which is why "*" stays available as an escape hatch.
     * <p>
     * {@code permissions}, {@code owners}, {@code driveId} and {@code hasAugmentedPermissions} carry
     * the whole ACL: they feed the three tiers of {@link DrivePermissionResolver#resolve}, and
     * dropping any of them silently strips the roles of every crawled document instead of failing.
     * </p>
     */
    protected static final String DEFAULT_FILE_FIELDS = "id,name,description,mimeType,size,kind,fileExtension,fullFileExtension,"
            + "originalFilename,md5Checksum,headRevisionId,iconLink,thumbnailLink,thumbnailVersion,hasThumbnail,webViewLink,"
            + "webContentLink,exportLinks,createdTime,modifiedTime,modifiedByMe,modifiedByMeTime,viewedByMe,viewedByMeTime,"
            + "trashed,explicitlyTrashed,trashedTime,trashingUser(emailAddress,displayName),parents,folderColorRgb,"
            + "owners(emailAddress,displayName),ownedByMe,lastModifyingUser(emailAddress,displayName),shared,driveId,teamDriveId,"
            + "permissions(id,type,role,emailAddress,domain,deleted,allowFileDiscovery,permissionDetails),permissionIds,"
            + "hasAugmentedPermissions,capabilities,quotaBytesUsed,version,writersCanShare,viewersCanCopyContent,"
            + "copyRequiresWriterPermission,isAppAuthorized,appProperties,contentHints,imageMediaMetadata,videoMediaMetadata";

    /**
     * Default constructor.
     */
    public GoogleDriveDataStore() {
        super();
    }

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * {@inheritDoc}
     * <p>
     * The incremental option has to be applied here rather than in {@link #storeData}:
     * {@code AbstractDataStore#store} hands {@code storeData} a copy of the parameter map, while
     * {@code DataIndexHelper} reads back the very {@code initParamMap} object it passed to this
     * method.
     * </p>
     * <p>
     * The suppression is written twice on purpose. {@code AbstractDataStore#store} merges the handler
     * parameters into {@code initParamMap}, so an explicit {@code delete_old_docs=true} would
     * otherwise overwrite the first write; and {@code DataIndexHelper} sweeps in a {@code finally}
     * block, so the second write has to happen even when the crawl failed.
     * </p>
     */
    @Override
    public void store(final DataConfig config, final IndexUpdateCallback callback, final DataStoreParams initParamMap) {
        if (!isIncrementalConfig(config)) {
            super.store(config, callback, initParamMap);
            return;
        }
        logger.info("Incremental crawling is enabled: the stale document sweep is disabled for this run.");
        disableDeleteOldDocsIfIncremental(config, initParamMap);
        try {
            super.store(config, callback, initParamMap);
        } finally {
            disableDeleteOldDocsIfIncremental(config, initParamMap);
        }
    }

    /**
     * Returns whether the data configuration asks for an incremental crawl.
     *
     * @param config The data configuration, possibly null.
     * @return true when incremental crawling is enabled.
     */
    protected boolean isIncrementalConfig(final DataConfig config) {
        if (config == null) {
            return false;
        }
        return Constants.TRUE.equalsIgnoreCase(config.getHandlerParameterMap().get(INCREMENTAL));
    }

    /**
     * Switches off the stale document sweep of {@code DataIndexHelper} when the crawl is incremental.
     * <p>
     * {@code DataIndexHelper#deleteOldDocs()} deletes every document of the configuration whose
     * segment differs from the current session id, which assumes a full crawl. An incremental run
     * only touches the documents that changed, so the sweep would delete the rest of the index.
     * </p>
     * <p>
     * An explicit {@code delete_old_docs=true} is overridden rather than honoured: combined with
     * {@code incremental=true} it is not a preference but a data loss, and the supported way to drop
     * documents that vanished from Drive is a separate {@code incremental=false} configuration whose
     * full crawl sweeps them. The override is logged so it is visible in the crawler log.
     * </p>
     *
     * @param config The data configuration, possibly null.
     * @param initParamMap The parameter map {@code DataIndexHelper} reads back.
     */
    protected void disableDeleteOldDocsIfIncremental(final DataConfig config, final DataStoreParams initParamMap) {
        if (!isIncrementalConfig(config)) {
            return;
        }
        final String configured = initParamMap.getAsString(DELETE_OLD_DOCS);
        if (StringUtil.isNotBlank(configured) && !Constants.FALSE.equalsIgnoreCase(configured)) {
            logger.warn("'{}={}' is ignored because '{}' is enabled: an incremental run only sees changed documents, so the stale "
                    + "document sweep would delete every document it did not touch. Schedule a separate '{}=false' configuration to "
                    + "drop the documents that vanished from Drive.", DELETE_OLD_DOCS, configured, INCREMENTAL, INCREMENTAL);
        }
        initParamMap.put(DELETE_OLD_DOCS, Constants.FALSE);
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(CRAWL_TARGET, getCrawlTarget(paramMap));
        configMap.put(MAX_SIZE, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        configMap.put(PERMISSION_RESOLVERS, new ConcurrentHashMap<GSuiteClient, DrivePermissionResolver>());
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        try (final GSuiteClient client = createClient(paramMap)) {
            storeFiles(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client);
        }
    }

    /**
     * Creates a GSuiteClient.
     * @param paramMap The parameters for the data store.
     * @return A GSuiteClient.
     */
    protected GSuiteClient createClient(final DataStoreParams paramMap) {
        return new GSuiteClient(paramMap);
    }

    /**
     * Returns whether to ignore folders.
     * @param paramMap The parameters for the data store.
     * @return true if folders should be ignored, false otherwise.
     */
    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
    }

    /**
     * Returns whether to ignore errors.
     * @param paramMap The parameters for the data store.
     * @return true if errors should be ignored, false otherwise.
     */
    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
    }

    /**
     * Returns the maximum size of a file to be indexed.
     * @param paramMap The parameters for the data store.
     * @return The maximum size of a file to be indexed.
     */
    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_SIZE);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
        } catch (final NumberFormatException e) {
            return DEFAULT_MAX_SIZE;
        }
    }

    /**
     * Returns how long to wait for the crawler thread pool to drain, in seconds.
     * @param paramMap The parameters for the data store.
     * @return The timeout in seconds.
     */
    protected long getThreadPoolTimeoutSeconds(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(THREAD_POOL_TIMEOUT_SECONDS);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_THREAD_POOL_TIMEOUT_SECONDS;
        } catch (final NumberFormatException e) {
            return DEFAULT_THREAD_POOL_TIMEOUT_SECONDS;
        }
    }

    /**
     * Returns the URL filter.
     * @param paramMap The parameters for the data store.
     * @return The URL filter.
     */
    protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
        final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
        final String include = paramMap.getAsString(INCLUDE_PATTERN);
        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
        }
        final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
        }
        urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
        if (logger.isDebugEnabled()) {
            logger.debug("urlFilter: {}", urlFilter);
        }
        return urlFilter;
    }

    /**
     * Returns the supported mime types.
     * @param paramMap The parameters for the data store.
     * @return The supported mime types.
     */
    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    /**
     * Returns the validated crawl target.
     * <p>
     * Every target other than {@code legacy} relies on {@code useDomainAdminAccess}, which the Drive
     * API only grants to a Google Workspace domain administrator. A service account is not one by
     * itself: it has to impersonate an administrator through domain-wide delegation. A missing
     * {@code impersonate_user} therefore fails the crawl at startup rather than silently indexing
     * zero files.
     * <p>
     * {@code users} and {@code both} additionally enumerate the directory through
     * {@code admin/directory/v1/users}, which needs
     * {@link GSuiteClient#ADMIN_DIRECTORY_USER_READONLY_SCOPE}. That scope is not part of
     * {@link GSuiteClient#DEFAULT_SCOPES}, so without it the combination can only fail with an
     * opaque 403 once the crawl has already begun. It is rejected here instead.
     *
     * @param paramMap The parameters for the data store.
     * @return One of {@code legacy}, {@code shared_drives}, {@code users} or {@code both}.
     */
    protected String getCrawlTarget(final DataStoreParams paramMap) {
        final String crawlTarget = paramMap.getAsString(CRAWL_TARGET, TARGET_SHARED_DRIVES).trim();
        if (!TARGET_LEGACY.equals(crawlTarget) && !TARGET_SHARED_DRIVES.equals(crawlTarget) && !TARGET_USERS.equals(crawlTarget)
                && !TARGET_BOTH.equals(crawlTarget)) {
            throw new DataStoreException("parameter '" + CRAWL_TARGET + "' must be one of '" + TARGET_LEGACY + "', '" + TARGET_SHARED_DRIVES
                    + "', '" + TARGET_USERS + "' or '" + TARGET_BOTH + "': " + crawlTarget);
        }
        if (!TARGET_LEGACY.equals(crawlTarget) && StringUtil.isBlank(paramMap.getAsString(IMPERSONATE_USER))) {
            throw new DataStoreException(
                    "parameter '" + IMPERSONATE_USER + "' is required when '" + CRAWL_TARGET + "' is not '" + TARGET_LEGACY + "'.");
        }
        if ((TARGET_USERS.equals(crawlTarget) || TARGET_BOTH.equals(crawlTarget))
                && !GSuiteClient.resolveScopes(paramMap).contains(GSuiteClient.ADMIN_DIRECTORY_USER_READONLY_SCOPE)) {
            throw new DataStoreException(
                    "parameter '" + GSuiteClient.SCOPES + "' must include '" + GSuiteClient.ADMIN_DIRECTORY_USER_READONLY_SCOPE + "' when '"
                            + CRAWL_TARGET + "' is '" + TARGET_USERS + "' or '" + TARGET_BOTH + "'.");
        }
        return crawlTarget;
    }

    /**
     * Returns the inner {@code files(...)} projection, that is the field list without the files.list
     * envelope. changes.list needs this form because it nests the file under {@code changes(file(...))}.
     *
     * @param paramMap The parameters for the data store.
     * @return The inner projection, or "*".
     */
    protected String getFileFieldProjection(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(FIELDS, DEFAULT_FILE_FIELDS);
        if (StringUtil.isBlank(value)) {
            return DEFAULT_FILE_FIELDS;
        }
        return value.trim();
    }

    /**
     * Builds the {@code fields} value handed to files.list.
     * <p>
     * "*" is passed through so an existing configuration keeps working, an already complete projection
     * is used verbatim, and a bare field list is wrapped in the files.list envelope so
     * {@code nextPageToken} and {@code incompleteSearch} are always returned (D-15).
     * </p>
     *
     * @param paramMap The parameters for the data store.
     * @return The projection for files.list.
     */
    protected String buildFileFields(final DataStoreParams paramMap) {
        final String value = getFileFieldProjection(paramMap);
        if (WILDCARD_FIELDS.equals(value)) {
            return WILDCARD_FIELDS;
        }
        if (value.contains("files(") || value.contains("nextPageToken")) {
            return value;
        }
        return "nextPageToken,incompleteSearch,files(" + value + ")";
    }

    /**
     * Creates a new fixed thread pool.
     * @param nThreads The number of threads.
     * @return A new fixed thread pool.
     */
    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Stores the files, choosing the traversal route from {@link #CRAWL_TARGET}.
     * <p>
     * When the configuration is incremental every scope is walked through its own change feed
     * instead of a full listing, and the start page tokens are persisted once the executor has
     * drained. They are <em>not</em> persisted when the crawl was stopped or the executor did not
     * drain in time: an advanced token would silently skip the changes whose files never reached the
     * index.
     * </p>
     * <p>
     * The tokens are checked against {@link #buildCrawlSignature} before anything is crawled, so a
     * configuration change discards them and the run degrades to a full crawl of every scope.
     * </p>
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index the files.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     */
    protected void storeFiles(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client) {
        final String crawlTarget = (String) configMap.get(CRAWL_TARGET);
        // A listing the client could not finish must not disappear: the crawl goes on, but the
        // operator has to find the failure in the log and in the failure URL list.
        client.setFailureHandler((target, e) -> handleClientFailure(dataConfig, target, e));
        // changes.list nests the file under changes(file(...)), so it needs the inner projection.
        client.setFileFields(getFileFieldProjection(paramMap));
        final DriveCrawlState state = isIncrementalConfig(dataConfig) ? newCrawlState(dataConfig) : null;
        // A stored token is only meaningful for the configuration it was taken in.
        applyCrawlSignature(state, paramMap);
        // A file shared with several users shows up once per viewpoint, so index it once.
        final Set<String> crawledFileIds = ConcurrentHashMap.newKeySet();
        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try {
            if (TARGET_LEGACY.equals(crawlTarget)) {
                crawlLegacy(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, executorService, crawledFileIds,
                        state);
            } else {
                if (TARGET_SHARED_DRIVES.equals(crawlTarget) || TARGET_BOTH.equals(crawlTarget)) {
                    crawlSharedDrives(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, executorService,
                            crawledFileIds, state);
                }
                if (TARGET_USERS.equals(crawlTarget) || TARGET_BOTH.equals(crawlTarget)) {
                    crawlUsers(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, executorService,
                            crawledFileIds, state);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            final boolean drained = executorService.awaitTermination(getThreadPoolTimeoutSeconds(paramMap), TimeUnit.SECONDS);
            if (state != null) {
                if (alive && drained) {
                    state.save();
                } else {
                    logger.warn("The crawl did not finish, so the start page tokens are left untouched "
                            + "and the next run reads the same changes again.");
                }
            }
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Loads the persisted crawl state of a data configuration. Exists as an override point for tests.
     *
     * @param dataConfig The data configuration.
     * @return The crawl state.
     */
    protected DriveCrawlState newCrawlState(final DataConfig dataConfig) {
        return new DriveCrawlState(dataConfig);
    }

    /**
     * Builds a single line fingerprint of the configuration that decides what a scope yields.
     * <p>
     * A stored start page token only describes the population it was taken over. Widening
     * {@code query}, switching {@code corpora} from {@code user} to {@code allDrives} or crawling as
     * a different {@code impersonate_user} all leave files that the previous run never indexed and
     * that no change feed will ever report, because they did not change. Resuming such a token skips
     * them permanently, so the signature has to invalidate it.
     * </p>
     * <p>
     * Only <em>configuration</em> is hashed, never the drives or users discovered at runtime. A
     * signature over discovered domain state would discard every scope's token whenever any single
     * drive or user appears or disappears, degrading every subsequent run to a full crawl of the
     * whole domain. Nothing is lost by leaving them out: a scope key with no stored token already
     * routes to {@link #crawlFullyAndAnchor} in {@link #storeScope}, so a new shared drive or a new
     * user is crawled in full and anchored on its own, and the token of a scope that disappeared is
     * simply never read again.
     * </p>
     * <p>
     * No parameter listed in {@link #SECRET_PARAM_KEYS} is an input. The result is written into the
     * {@code handlerParameter} of the data configuration, which is stored in the config index and
     * rendered in the admin UI, so a credential must not be derivable from it -- not even through a
     * hash. The inputs are therefore an explicit allowlist rather than the parameter map minus the
     * secrets: a later parameter that carries a credential is then excluded by default instead of
     * being hashed until someone remembers to register it.
     * </p>
     *
     * @param paramMap The parameters for the data store.
     * @return The signature, as SHA-256 hex.
     */
    protected String buildCrawlSignature(final DataStoreParams paramMap) {
        // getCrawlTarget trims, so " both" and "both" select the very same traversal.
        final String source = String.join("\n", //
                CRAWL_TARGET + "=" + paramMap.getAsString(CRAWL_TARGET, TARGET_SHARED_DRIVES).trim(), //
                IMPERSONATE_USER + "=" + paramMap.getAsString(IMPERSONATE_USER, StringUtil.EMPTY), //
                USER_QUERY + "=" + paramMap.getAsString(USER_QUERY, StringUtil.EMPTY), //
                "query=" + paramMap.getAsString("query", StringUtil.EMPTY), //
                "corpora=" + paramMap.getAsString("corpora", GSuiteClient.ALL_DRIVES), //
                "spaces=" + paramMap.getAsString("spaces", StringUtil.EMPTY));
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (final NoSuchAlgorithmException e) {
            throw new DataStoreException("SHA-256 is not available.", e);
        }
    }

    /**
     * Discards the stored start page tokens when the crawl configuration changed, then records the
     * new signature.
     * <p>
     * Absent, unreadable and mismatched are all treated the same way, because only a signature that
     * is present and equal proves the tokens describe the current configuration. Discarding is the
     * safe direction: it costs one full crawl, while a wrong resume is a silent and permanent hole in
     * the index. It also cannot delete anything, since {@link #store} has already forced
     * {@code delete_old_docs} to false for every incremental run, so the fallback full crawl
     * re-indexes without the stale document sweep ever running.
     * </p>
     * <p>
     * Nothing is persisted here. The state is only written back by {@link DriveCrawlState#save()},
     * which {@link #storeFiles} calls solely when the crawl finished, so a run that was stopped
     * leaves the previous signature and the previous tokens in place and the next run simply repeats
     * this decision.
     * </p>
     *
     * @param state The persisted crawl state, or null when the crawl is not incremental.
     * @param paramMap The parameters for the data store.
     */
    protected void applyCrawlSignature(final DriveCrawlState state, final DataStoreParams paramMap) {
        if (state == null) {
            return;
        }
        final String signature = buildCrawlSignature(paramMap);
        if (!state.isCompatible(signature)) {
            logger.info("The crawl configuration changed. Discarding the stored start page tokens and crawling every scope in full.");
            state.clearTokens();
        }
        state.setSignature(signature);
    }

    /**
     * Records a Drive listing failure that the client did not propagate, so that the crawl continues
     * while the operator still sees which drive or user failed.
     *
     * @param dataConfig The data configuration.
     * @param target The description of the failed listing.
     * @param e The failure.
     */
    protected void handleClientFailure(final DataConfig dataConfig, final String target, final Exception e) {
        logger.warn("Failed to access {}. Continuing the crawl without it.", target, e);
        try {
            ComponentUtil.getComponent(FailureUrlService.class).store(dataConfig, e.getClass().getCanonicalName(), target, e);
        } catch (final Exception ex) {
            logger.warn("Failed to record the failure of {}.", target, ex);
        }
    }

    /**
     * Walks the files visible to the service account itself, as releases before 15.9 did.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index the files.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     * @param executorService The executor that runs the per-file work.
     * @param crawledFileIds The file IDs already submitted.
     * @param state The persisted crawl state, or null when the crawl is not incremental.
     */
    protected void crawlLegacy(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final ExecutorService executorService, final Set<String> crawledFileIds,
            final DriveCrawlState state) {
        final String query = paramMap.getAsString("query");
        final String corpora = paramMap.getAsString("corpora", GSuiteClient.ALL_DRIVES);
        final String spaces = paramMap.getAsString("spaces");
        final String fields = buildFileFields(paramMap);
        storeScope(client, state, DriveCrawlState.userScopeKey(TARGET_LEGACY), null,
                newChangeConsumer(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, executorService,
                        crawledFileIds),
                () -> client.getFiles(query, corpora, spaces, fields, file -> submitFile(dataConfig, callback, configMap, paramMap,
                        scriptMap, defaultDataMap, client, executorService, crawledFileIds, file)));
    }

    /**
     * Walks every shared drive of the domain, one {@code corpora=drive} listing per drive.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index the files.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     * @param executorService The executor that runs the per-file work.
     * @param crawledFileIds The file IDs already submitted.
     * @param state The persisted crawl state, or null when the crawl is not incremental.
     */
    protected void crawlSharedDrives(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final ExecutorService executorService, final Set<String> crawledFileIds,
            final DriveCrawlState state) {
        final String query = paramMap.getAsString("query");
        final String fields = buildFileFields(paramMap);
        final Consumer<Change> changeConsumer = newChangeConsumer(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                client, executorService, crawledFileIds);
        client.getDrives(sharedDrive -> {
            if (!alive) {
                // The admin UI asked this data store to stop; do not start another listing.
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling a shared drive: {} ({})", sharedDrive.getName(), sharedDrive.getId());
            }
            storeScope(client, state, DriveCrawlState.driveScopeKey(sharedDrive.getId()), sharedDrive.getId(), changeConsumer,
                    () -> client.getFilesInDrive(sharedDrive.getId(), query, fields, file -> submitFile(dataConfig, callback, configMap,
                            paramMap, scriptMap, defaultDataMap, client, executorService, crawledFileIds, file)));
        });
    }

    /**
     * Walks the My Drive of every directory user by impersonating each of them in turn.
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index the files.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     * @param executorService The executor that runs the per-file work.
     * @param crawledFileIds The file IDs already submitted.
     * @param state The persisted crawl state, or null when the crawl is not incremental.
     */
    protected void crawlUsers(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final ExecutorService executorService, final Set<String> crawledFileIds,
            final DriveCrawlState state) {
        final String query = paramMap.getAsString("query");
        final String spaces = paramMap.getAsString("spaces");
        final String fields = buildFileFields(paramMap);
        for (final String userEmail : client.listUsers(paramMap.getAsString(USER_QUERY))) {
            if (!alive) {
                // The admin UI asked this data store to stop; do not start another listing.
                return;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling the My Drive of {}", userEmail);
            }
            // Not closed on purpose: the per-user client borrows the transport of the primary
            // client, which storeData closes, and owns nothing else that has to be released.
            final GSuiteClient userClient = client.forUser(userEmail);
            // forUser rebuilds the client from the parameters, so the projection is set again here.
            userClient.setFileFields(getFileFieldProjection(paramMap));
            storeScope(userClient, state, DriveCrawlState.userScopeKey(userEmail), null,
                    newChangeConsumer(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, userClient, executorService,
                            crawledFileIds),
                    () -> userClient.getFiles(query, GSuiteClient.USER_CORPORA, spaces, fields, file -> submitFile(dataConfig, callback,
                            configMap, paramMap, scriptMap, defaultDataMap, userClient, executorService, crawledFileIds, file)));
        }
    }

    /**
     * Crawls one scope, either fully or through its change feed.
     * <p>
     * A scope is one shared drive ({@code driveId} set) or the view of one impersonated user
     * ({@code driveId} null). Each has its own change feed and therefore its own start page token.
     * </p>
     *
     * @param client The GSuiteClient bound to this scope.
     * @param state The persisted crawl state, or null when the crawl is not incremental.
     * @param scopeKey The scope key of this scope.
     * @param driveId The shared drive id, or null for a user scope.
     * @param changeConsumer The consumer applied to each change.
     * @param fullCrawl The full listing of this scope.
     */
    protected void storeScope(final GSuiteClient client, final DriveCrawlState state, final String scopeKey, final String driveId,
            final Consumer<Change> changeConsumer, final Runnable fullCrawl) {
        if (state == null) {
            fullCrawl.run();
            return;
        }
        final String pageToken = state.getToken(scopeKey);
        if (StringUtil.isBlank(pageToken)) {
            logger.info("No start page token for {}. Crawling it fully and taking a token for the next run.", scopeKey);
            crawlFullyAndAnchor(client, state, scopeKey, driveId, fullCrawl);
            return;
        }
        try {
            final String newStartPageToken = client.getChanges(pageToken, driveId, changeConsumer);
            if (StringUtil.isNotBlank(newStartPageToken)) {
                state.putToken(scopeKey, newStartPageToken);
            }
        } catch (final Exception e) {
            // The feed lost a page, so the deltas it would have carried are gone. Only a full
            // listing can bring the scope back in sync.
            logger.warn("Failed to read the changes of {}. Crawling it fully and taking a new token.", scopeKey, e);
            state.removeToken(scopeKey);
            crawlFullyAndAnchor(client, state, scopeKey, driveId, fullCrawl);
        }
    }

    /**
     * Lists a scope in full and anchors its change feed for the next run.
     * <p>
     * The token is taken <em>before</em> the listing: a file changed while the listing runs is then
     * replayed by the next run, at worst indexed twice, instead of being missed. A scope whose token
     * could not be taken is left unanchored, so the next run lists it in full again rather than
     * resuming from a point it never reached.
     * </p>
     *
     * @param client The GSuiteClient bound to this scope.
     * @param state The persisted crawl state.
     * @param scopeKey The scope key of this scope.
     * @param driveId The shared drive id, or null for a user scope.
     * @param fullCrawl The full listing of this scope.
     */
    protected void crawlFullyAndAnchor(final GSuiteClient client, final DriveCrawlState state, final String scopeKey, final String driveId,
            final Runnable fullCrawl) {
        String token = null;
        try {
            token = client.getStartPageToken(driveId);
        } catch (final Exception e) {
            logger.warn("Failed to take a start page token for {}. The next run crawls it fully again.", scopeKey, e);
        }
        fullCrawl.run();
        if (token != null) {
            state.putToken(scopeKey, token);
        } else {
            state.removeToken(scopeKey);
        }
    }

    /**
     * Builds the consumer applied to every change of one scope.
     * <p>
     * A change either drops a document from the index or submits the file for indexing through the
     * same path as a full listing, so the file ID de-duplication also covers a file that changed in
     * several scopes.
     * </p>
     * <p>
     * A change feed is per scope and {@code removed} covers a loss of access as well as a deletion,
     * so with {@link #TARGET_USERS} or {@link #TARGET_BOTH} a file that one user may no longer read
     * is dropped from the index even when another user still can. It comes back on the next change
     * to that file, or on the next full crawl.
     * </p>
     *
     * @param dataConfig The data configuration.
     * @param callback The callback to index the files.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient bound to this scope.
     * @param executorService The executor that runs the per-file work.
     * @param crawledFileIds The file IDs already submitted.
     * @return The change consumer.
     */
    protected Consumer<Change> newChangeConsumer(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final GSuiteClient client, final ExecutorService executorService,
            final Set<String> crawledFileIds) {
        return change -> {
            if (!alive) {
                // The admin UI asked this data store to stop; do not apply any more changes.
                return;
            }
            final File file = change.getFile();
            if (isDeletion(change)) {
                if (file != null) {
                    deleteFile(configMap, paramMap, file);
                } else {
                    deleteFileById(dataConfig, change.getFileId());
                }
                return;
            }
            if (file == null) {
                // A drive level change carries no file, so there is nothing to index.
                if (logger.isDebugEnabled()) {
                    logger.debug("Skipping a change with no file: {}", change.getChangeType());
                }
                return;
            }
            submitFile(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, executorService, crawledFileIds, file);
        };
    }

    /**
     * Returns whether a change means the document must leave the index.
     *
     * @param change The change.
     * @return true when the file was removed or moved to the trash.
     */
    protected boolean isDeletion(final Change change) {
        if (Boolean.TRUE.equals(change.getRemoved())) {
            return true;
        }
        final File file = change.getFile();
        return file != null && Boolean.TRUE.equals(file.getTrashed());
    }

    /**
     * Drops the document of a file the change still describes.
     * <p>
     * The URL is the one {@link #getUrl} produced when the file was indexed, so the deletion is an
     * exact term match and costs no wildcard. A configuration whose {@link #FIELDS} projection drops
     * {@code webViewLink} indexes the fallback URL instead, and both forms go through
     * {@link #getUrl}, so the two stay in step.
     * </p>
     *
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param file The removed file.
     */
    protected void deleteFile(final Map<String, Object> configMap, final DataStoreParams paramMap, final File file) {
        final String url = getUrl(configMap, paramMap, file);
        if (StringUtil.isBlank(url)) {
            return;
        }
        try {
            final long deleted = deleteDocumentByUrl(url);
            if (logger.isDebugEnabled()) {
                logger.debug("Deleted {} documents. (url: {})", deleted, url);
            }
        } catch (final Exception e) {
            logger.warn("Failed to delete the document. (url: {})", url, e);
        }
    }

    /**
     * Drops the document of a file that is only known by its id.
     * <p>
     * A removed change carries no {@code File}, so the URL that was indexed cannot be reproduced: it
     * is the {@code webViewLink}, whose shape depends on the mime type
     * ({@code docs.google.com/document/d/<id>/edit} for a document,
     * {@code .../spreadsheets/d/<id>/edit} for a spreadsheet, and so on). The document is therefore
     * matched on the file id appearing anywhere in its URL.
     * </p>
     * <p>
     * The {@code config_id} filter is not optional: without it a Drive file id that also occurs in
     * the URL of another data store's document would delete that document too. A leading wildcard is
     * expensive, but the number of these queries is bounded by the number of changes, not by the size
     * of the index.
     * </p>
     * <p>
     * The residual limit is that the indexed URL must contain the file id. That holds for
     * {@code webViewLink} and for the fallback URL, but not if a crawling script rewrites {@code url}
     * to a value the id does not appear in; such a document can only be removed by a full crawl with
     * {@code delete_old_docs} enabled.
     * </p>
     *
     * @param dataConfig The data configuration, whose config id scopes the deletion.
     * @param fileId The Drive file id.
     */
    protected void deleteFileById(final DataConfig dataConfig, final String fileId) {
        if (StringUtil.isBlank(fileId)) {
            return;
        }
        final String configId = dataConfig == null ? null : dataConfig.getConfigId();
        if (StringUtil.isBlank(configId)) {
            logger.warn("Cannot delete the document of {}: the data configuration has no config id, "
                    + "and an unscoped match would reach the documents of other data stores.", fileId);
            return;
        }
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(fessConfig.getIndexFieldConfigId(), configId))
                .filter(QueryBuilders.wildcardQuery(fessConfig.getIndexFieldUrl(), "*" + fileId + "*"));
        try {
            final long deleted = deleteDocumentByQuery(queryBuilder);
            if (logger.isDebugEnabled()) {
                logger.debug("Deleted {} documents. (fileId: {})", deleted, fileId);
            }
        } catch (final Exception e) {
            logger.warn("Failed to delete the documents. (fileId: {})", fileId, e);
        }
    }

    /**
     * Deletes every indexed document with the given URL. Separated so tests can capture the deletion.
     *
     * @param url The URL of the documents to delete.
     * @return The number of deleted documents.
     */
    protected long deleteDocumentByUrl(final String url) {
        return ComponentUtil.getIndexingHelper().deleteDocumentByUrl(ComponentUtil.getSearchEngineClient(), url);
    }

    /**
     * Deletes every indexed document matching the given query. Separated so tests can capture the
     * deletion.
     *
     * @param queryBuilder The query.
     * @return The number of deleted documents.
     */
    protected long deleteDocumentByQuery(final QueryBuilder queryBuilder) {
        return ComponentUtil.getIndexingHelper().deleteDocumentByQuery(ComponentUtil.getSearchEngineClient(), queryBuilder);
    }

    /**
     * Submits one file for processing unless the crawl was stopped or the file was already seen.
     * <p>
     * The de-duplication is the single atomic {@link Set#add(Object)} on a concurrent set: a
     * {@code contains} followed by an {@code add} would let two listing threads submit the same
     * file, and the same document would then be indexed twice.
     * </p>
     * @param dataConfig The data configuration.
     * @param callback The callback to index the file.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The client that produced the file.
     * @param executorService The executor that runs the per-file work.
     * @param crawledFileIds The file IDs already submitted.
     * @param file The file.
     */
    protected void submitFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final ExecutorService executorService, final Set<String> crawledFileIds, final File file) {
        if (!alive) {
            // The admin UI asked this data store to stop; do not queue any more work.
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling is stopped. Skipping {}.", file.getId());
            }
            return;
        }
        final String fileId = file.getId();
        if (StringUtil.isNotBlank(fileId) && !crawledFileIds.add(fileId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipped a duplicated file: {}", fileId);
            }
            return;
        }
        executorService.execute(() -> processFile(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, file));
    }

    /**
     * Checks if a file should be processed based on filtering rules.
     * @param file The file to check.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param statsKey The stats key for tracking.
     * @param crawlerStatsHelper The crawler stats helper.
     * @return true if the file should be processed, false otherwise.
     */
    protected boolean shouldProcessFile(final File file, final Map<String, Object> configMap, final DataStoreParams paramMap,
            final StatsKeyObject statsKey, final CrawlerStatsHelper crawlerStatsHelper) {
        final String mimetype = file.getMimeType();

        // Check if folder should be ignored
        if (((Boolean) configMap.get(IGNORE_FOLDER)).booleanValue() && "application/vnd.google-apps.folder".equals(mimetype)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignore item: {}", file.getWebContentLink());
            }
            crawlerStatsHelper.discard(statsKey);
            return false;
        }

        // Check supported MIME types
        final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);
        if (!Stream.of(supportedMimeTypes).anyMatch(mimetype::matches)) {
            if (logger.isDebugEnabled()) {
                logger.debug("{} is not an indexing target.", mimetype);
            }
            crawlerStatsHelper.discard(statsKey);
            return false;
        }

        // Check URL filter
        final String url = getUrl(configMap, paramMap, file);
        final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
        if (urlFilter != null && !urlFilter.match(url)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Not matched: {}", url);
            }
            crawlerStatsHelper.discard(statsKey);
            return false;
        }

        return true;
    }

    /**
     * Builds the file metadata map.
     * @param file The file to extract metadata from.
     * @param content The file content.
     * @param size The file size.
     * @param url The file URL.
     * @return The file metadata map.
     */
    protected Map<String, Object> buildFileMap(final File file, final String content, final long size, final String url) {
        final String mimetype = file.getMimeType();
        final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
        final Map<String, Object> fileMap = new HashMap<>();

        fileMap.put(FILE_NAME, file.getName());
        fileMap.put(FILE_DESCRIPTION, file.getDescription() != null ? file.getDescription() : "");
        fileMap.put(FILE_CONTENTS, content);
        fileMap.put(FILE_MIMETYPE, mimetype);
        fileMap.put(FILE_FILETYPE, filetype);
        fileMap.put(FILE_SIZE, size);
        fileMap.put(FILE_WEB_VIEW_LINK, file.getWebViewLink());
        fileMap.put(FILE_WEB_CONTENT_LINK, file.getWebContentLink());
        fileMap.put(FILE_URL, url);
        fileMap.put(FILE_CLASS_INFO, file.getClassInfo());
        fileMap.put(FILE_CONTENT_HINTS, file.getContentHints());
        fileMap.put(FILE_CAPABILITIES, file.getCapabilities());
        fileMap.put(FILE_APP_PROPERTIES, file.getAppProperties());
        fileMap.put(FILE_COPY_REQUIRES_WRITER_PERMISSION, file.getCopyRequiresWriterPermission());
        fileMap.put(FILE_EXPLICITLY_TRASHED, file.getExplicitlyTrashed());
        fileMap.put(FILE_EXPORT_LINKS, file.getExportLinks());
        fileMap.put(FILE_FILE_EXTENSION, file.getFileExtension());
        fileMap.put(FILE_FOLDER_COLOR_RGB, file.getFolderColorRgb());
        fileMap.put(FILE_FULL_FILE_EXTENSION, file.getFullFileExtension());
        fileMap.put(FILE_HAS_AUGMENTED_PERMISSIONS, file.getHasAugmentedPermissions());
        fileMap.put(FILE_HAS_THUMBNAIL, file.getHasThumbnail());
        fileMap.put(FILE_HEAD_REVISION_ID, file.getHeadRevisionId());
        fileMap.put(FILE_ICON_LINK, file.getIconLink());
        fileMap.put(FILE_ID, file.getId());
        fileMap.put(FILE_IMAGE_MEDIA_METADATA, file.getImageMediaMetadata());
        fileMap.put(FILE_IS_APP_AUTHORIZED, file.getIsAppAuthorized());
        fileMap.put(FILE_KIND, file.getKind());
        fileMap.put(FILE_LAST_MODIFYING_USER, file.getLastModifyingUser());
        fileMap.put(FILE_MD5_CHECKSUM, file.getMd5Checksum());
        fileMap.put(FILE_MODIFIED_BY_ME, file.getModifiedByMe());
        fileMap.put(FILE_MODIFIED_BY_ME_TIME, toDate(file.getModifiedByMeTime()));
        fileMap.put(FILE_ORIGINAL_FILENAME, file.getOriginalFilename());
        fileMap.put(FILE_OWNED_BY_ME, file.getOwnedByMe());
        fileMap.put(FILE_OWNERS, file.getOwners());
        fileMap.put(FILE_PARENTS, file.getParents());
        fileMap.put(FILE_QUOTA_BYTES_USED, file.getQuotaBytesUsed());
        fileMap.put(FILE_SHARED, file.getShared());
        fileMap.put(FILE_TEAM_DRIVE_ID, file.getTeamDriveId());
        fileMap.put(FILE_THUMBNAIL_VERSION, file.getThumbnailVersion());
        fileMap.put(FILE_TRASHED, file.getTrashed());
        fileMap.put(FILE_TRASHED_TIME, toDate(file.getTrashedTime()));
        fileMap.put(FILE_TRASHING_USER, file.getTrashingUser());
        fileMap.put(FILE_VERSION, file.getVersion());
        fileMap.put(FILE_VIDEO_MEDIA_METADATA, file.getVideoMediaMetadata());
        fileMap.put(FILE_VIEWED_BY_ME, file.getViewedByMe());
        fileMap.put(FILE_VIEWED_BY_ME_TIME, toDate(file.getViewedByMeTime()));
        fileMap.put(FILE_VIEWERS_CAN_COPY_CONTENT, file.getViewersCanCopyContent());
        fileMap.put(FILE_WRITERS_CAN_SHARE, file.getWritersCanShare());
        fileMap.put(FILE_THUMBNAIL_LINK, file.getThumbnailLink());
        fileMap.put(FILE_CREATED_TIME, toDate(file.getCreatedTime()));
        fileMap.put(FILE_MODIFIED_TIME, toDate(file.getModifiedTime()));

        return fileMap;
    }

    /**
     * Handles errors during file processing.
     * @param dataConfig The data configuration.
     * @param file The file being processed.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param dataMap The data map.
     * @param statsKey The stats key.
     * @param crawlerStatsHelper The crawler stats helper.
     * @param t The throwable that was caught.
     */
    protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
            final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {

        if (t instanceof CrawlingAccessException) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);

            Throwable target = t;
            if (target instanceof MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            String url = getUrl(configMap, paramMap, file);
            if (url == null) {
                url = StringUtil.EMPTY;
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, url, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } else {
            String url = getUrl(configMap, paramMap, file);
            if (url == null) {
                url = StringUtil.EMPTY;
            }

            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        }
    }

    /**
     * Processes a file.
     * <p>
     * The stats key and everything derived from it go on a per-call copy of {@code paramMap};
     * this method runs on the crawler thread pool and the caller's instance is shared by every
     * thread, so writing to it races when {@code number_of_threads} is greater than 1.
     * </p>
     * @param dataConfig The data configuration.
     * @param callback The callback to index the file.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store. Treated as read-only.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     * @param file The file to process.
     */
    protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final File file) {
        if (!alive) {
            // Work already queued when the stop request arrived must be dropped, not indexed.
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling is stopped. Skipping {}.", file.getId());
            }
            return;
        }
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("file: {}", file);
        }
        final StatsKeyObject statsKey = new StatsKeyObject(file.getId());
        final DataStoreParams localParamMap = paramMap.newInstance();
        localParamMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            crawlerStatsHelper.begin(statsKey);

            // Check if file should be processed (folder filtering, MIME type, URL filter)
            if (!shouldProcessFile(file, configMap, localParamMap, statsKey, crawlerStatsHelper)) {
                return;
            }

            final String url = getUrl(configMap, localParamMap, file);

            // Resolve the ACL before downloading anything: a document that is going to be skipped
            // must not cost an export or a Tika extraction.
            final List<String> permissions = getFilePermissions(configMap, localParamMap, client, file);
            if (permissions.isEmpty()) {
                logger.warn("Skipped {} because no permission could be resolved. "
                        + "Set default_permissions to index documents whose ACL is empty.", url);
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            logger.info("Crawling URL: {}", url);

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));

            // The script context is evaluated with arbitrary user-supplied expressions and its values
            // can be indexed, so the service account credentials must never reach it.
            final Map<String, Object> resultMap = new LinkedHashMap<>(localParamMap.asMap());
            for (final String secretKey : SECRET_PARAM_KEYS) {
                resultMap.remove(secretKey);
            }

            // Check the size Drive reports before spending a download and a Tika extraction on it
            final long maxSize = ((Long) configMap.get(MAX_SIZE)).longValue();
            final Long declaredSize = file.getSize();
            if (declaredSize != null && declaredSize.longValue() > maxSize) {
                throw new MaxLengthExceededException(
                        "The content length (" + declaredSize + " byte) is over " + maxSize + " byte. The url is " + url);
            }

            // Extract file content
            final String content = getFileContents(client, file, ignoreError);
            final long size;
            if (declaredSize != null) {
                size = declaredSize.longValue();
            } else if (content != null) {
                size = content.length();
            } else {
                size = 0;
            }

            // Google native formats report no size, so they can only be checked after extraction
            if (declaredSize == null && size > maxSize) {
                throw new MaxLengthExceededException(
                        "The content length (" + size + " byte) is over " + maxSize + " byte. The url is " + url);
            }

            // Build file metadata map
            final Map<String, Object> fileMap = buildFileMap(file, content, size, url);
            fileMap.put(FILE_ROLES, permissions);

            resultMap.put(FILE, fileMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            final String scriptType = getScriptType(localParamMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(localParamMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final Throwable t) {
            handleProcessingError(dataConfig, file, configMap, localParamMap, dataMap, statsKey, crawlerStatsHelper, t);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Converts a DateTime to a Date.
     * @param date The DateTime to convert.
     * @return The converted Date.
     */
    protected Date toDate(final com.google.api.client.util.DateTime date) {
        if (date == null) {
            return null;
        }
        return new Date(date.getValue());
    }

    /**
     * Returns the search roles to attach to the given file, applying the fail-closed rule.
     * <p>
     * The resolved Drive ACL wins. When it is empty, {@link #DEFAULT_PERMISSIONS} is used instead
     * -- it is a fallback, not an addition. When that is empty too the returned list is empty, and
     * the caller must skip the document: indexing a document with no role at all disables the Fess
     * permission filter for it and makes it visible to every user.
     * </p>
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param client The client that produced the file.
     * @param file The file.
     * @return The search roles. Never null, but possibly empty.
     */
    protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
            final GSuiteClient client, final File file) {
        final List<String> permissions = new ArrayList<>(getPermissionResolver(configMap, client, paramMap).resolve(file));
        if (!permissions.isEmpty()) {
            return permissions;
        }
        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                .of(stream -> stream.filter(StringUtil::isNotBlank)
                        .map(permissionHelper::encode)
                        .filter(StringUtil::isNotBlank)
                        .forEach(permissions::add));
        return permissions;
    }

    /**
     * Returns the resolver bound to the given client, creating it on first use.
     * <p>
     * The resolver is keyed by client because its shared drive ACL cache and its per-file
     * {@code permissions.list} calls must run with the identity that fetched the file.
     * </p>
     * @param configMap The configuration map, holding the resolver cache under
     *            {@link #PERMISSION_RESOLVERS}.
     * @param client The client. Must not be null.
     * @param paramMap The parameters for the data store.
     * @return The resolver.
     */
    protected DrivePermissionResolver getPermissionResolver(final Map<String, Object> configMap, final GSuiteClient client,
            final DataStoreParams paramMap) {
        @SuppressWarnings("unchecked")
        final Map<GSuiteClient, DrivePermissionResolver> resolvers =
                (Map<GSuiteClient, DrivePermissionResolver>) configMap.get(PERMISSION_RESOLVERS);
        return resolvers.computeIfAbsent(client, c -> new DrivePermissionResolver(c, paramMap));
    }

    /**
     * Returns the URL for a file.
     * <p>
     * {@code webViewLink} opens the file in a browser, which is what a search result must link to.
     * {@code webContentLink} is a direct-download link and is not usable for every mime type, so it
     * is no longer the default. When the file has no {@code webViewLink}, the canonical
     * {@code https://drive.google.com/open?id=<id>} form is used instead.
     * </p>
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param file The file.
     * @return The URL for the file, or null when neither a web view link nor an ID is available.
     */
    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final File file) {
        final String webViewLink = file.getWebViewLink();
        if (StringUtil.isNotBlank(webViewLink)) {
            return webViewLink;
        }
        final String id = file.getId();
        if (StringUtil.isNotBlank(id)) {
            return getFallbackUrl(id);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("id is null.");
        }
        return null;
    }

    /**
     * Returns the URL used when a file has no {@code webViewLink}. Indexing and deletion must agree
     * on this form, so both reach it through {@link #getUrl}.
     *
     * @param id The Drive file id.
     * @return The fallback URL.
     */
    protected String getFallbackUrl(final String id) {
        return "https://drive.google.com/open?id=" + id;
    }

    /**
     * Chooses the export target for a native Google type.
     * <p>
     * Google Forms and Google Sites have no row in {@code exportFormats} at all, so this returns null
     * for them and the caller indexes the metadata only. Falling through to {@code alt=media} for a
     * native type always fails, which is what the previous implementation did on every crawl (D-16).
     * </p>
     *
     * @param mimeType The source mime type.
     * @param exportFormats The live export format map from about.get.
     * @return The export target mime type, or null when the type cannot be exported.
     */
    protected String selectExportMimeType(final String mimeType, final Map<String, List<String>> exportFormats) {
        if (exportFormats == null) {
            return null;
        }
        final List<String> supported = exportFormats.get(mimeType);
        if (supported == null || supported.isEmpty()) {
            return null;
        }
        for (final String preferred : PREFERRED_EXPORT_MIMETYPES.getOrDefault(mimeType, List.of())) {
            if (supported.contains(preferred)) {
                return preferred;
            }
        }
        return supported.get(0);
    }

    /**
     * Flattens the Apps Script JSON bundle into "file name then source" for each script file.
     * Malformed JSON yields an empty string rather than aborting the file.
     *
     * @param json The exported Apps Script bundle.
     * @return The indexable text.
     */
    protected String extractScriptSource(final String json) {
        final StringBuilder sb = new StringBuilder();
        try {
            final Map<String, Object> map = new ObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {
            });
            if (map.containsKey("files")) {
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> files = (List<Map<String, Object>>) map.get("files");
                files.forEach(f -> {
                    sb.append(f.getOrDefault("name", StringUtil.EMPTY));
                    sb.append('\n');
                    sb.append(f.getOrDefault("source", StringUtil.EMPTY));
                    sb.append('\n');
                });
            }
        } catch (final Exception e) {
            logger.warn("Failed to parse an Apps Script bundle.", e);
        }
        return sb.toString();
    }

    /**
     * Returns the contents of a file.
     * <p>
     * A native Google type is exported, with the target chosen from the live
     * {@code exportFormats} map rather than from a hardcoded switch; a type with no target at all,
     * such as a Form or a Site, yields an empty string and is indexed with its metadata only.
     * Anything else is downloaded and handed to the Tika extractor.
     * </p>
     * @param client The GSuiteClient.
     * @param file The file.
     * @param ignoreError Whether to ignore errors.
     * @return The contents of the file.
     */
    protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
        final String mimeType = file.getMimeType();
        final String id = file.getId();

        // Native Google types must be exported; alt=media never works for them. A type that has no
        // export target at all (Forms, Sites) is not a failure, so it must not be reported as one:
        // a domain full of Forms would fill the admin failure list on every crawl.
        if (mimeType != null && mimeType.startsWith(GOOGLE_APPS_MIMETYPE_PREFIX)) {
            final String exportMimeType = selectExportMimeType(mimeType, client.getExportFormats());
            if (exportMimeType == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("No export format for {} ({}). Indexing the metadata only.", file.getName(), mimeType);
                }
                return StringUtil.EMPTY;
            }
            if (SCRIPT_EXPORT_MIMETYPE.equals(exportMimeType)) {
                return extractScriptSource(client.extractFileText(id, exportMimeType));
            }
            if (exportMimeType.startsWith("image/")) {
                // Drawings only export as an image; there is no text to index.
                return StringUtil.EMPTY;
            }
            return client.extractFileText(id, exportMimeType);
        }

        try (final InputStream in = client.getFileInputStream(id)) {
            return ComponentUtil.getExtractorFactory()
                    .builder(in, null)
                    .mimeType(mimeType)
                    .extractorName(extractorName)
                    .extract()
                    .getContent();
        } catch (final Exception e) {
            if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(file.getWebContentLink(), "Failed to get contents: " + file.getName(), e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents: {}", file.getName(), e);
            } else {
                logger.warn("Failed to get contents: {}. {}", file.getName(), e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }

    /**
     * Sets the name of the extractor to use.
     * @param extractorName The name of the extractor to use.
     */
    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}

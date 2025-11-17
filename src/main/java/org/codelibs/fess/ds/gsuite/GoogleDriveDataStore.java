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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.drive.model.User;

/**
 * DataStore for Google Drive.
 */
public class GoogleDriveDataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(GoogleDriveDataStore.class);

    /** Default maximum size of a file to be indexed. */
    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    /** Default thread pool termination timeout in seconds. */
    protected static final long DEFAULT_THREAD_POOL_TIMEOUT_SECONDS = 60L;

    /** Pattern for matching Google Apps MIME types. */
    protected static final Pattern GOOGLE_APPS_MIMETYPE_PATTERN = Pattern.compile("application/vnd\\.google-apps\\.(.*)");

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
    /** The fields to retrieve for files. */
    protected static final String FILE_FIELDS = "*";

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

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(MAX_SIZE, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
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
     * Stores the files.
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
        final String query = paramMap.getAsString("query");
        final String corpora = paramMap.getAsString("corpora", GSuiteClient.ALL_DRIVES);
        final String spaces = paramMap.getAsString("spaces");
        final String fields = paramMap.getAsString("fields", FILE_FIELDS);
        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try {
            client.getFiles(query, corpora, spaces, fields, file -> {
                executorService
                        .execute(() -> processFile(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, file));

            });
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            executorService.awaitTermination(DEFAULT_THREAD_POOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
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
     * @param dataConfig The data configuration.
     * @param callback The callback to index the file.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The GSuiteClient.
     * @param file The file to process.
     */
    protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final GSuiteClient client, final File file) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("file: {}", file);
        }
        final StatsKeyObject statsKey = new StatsKeyObject(file.getId());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
            crawlerStatsHelper.begin(statsKey);

            // Check if file should be processed (folder filtering, MIME type, URL filter)
            if (!shouldProcessFile(file, configMap, paramMap, statsKey, crawlerStatsHelper)) {
                return;
            }

            final String url = getUrl(configMap, paramMap, file);
            logger.info("Crawling URL: {}", url);

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());

            // Extract file content
            final String content = getFileContents(client, file, ignoreError);
            final long size;
            if (file.getSize() != null) {
                size = file.getSize();
            } else if (content != null) {
                size = content.length();
            } else {
                size = 0;
            }

            // Check file size
            if (size > ((Long) configMap.get(MAX_SIZE)).longValue()) {
                throw new MaxLengthExceededException(
                        "The content length (" + size + " byte) is over " + configMap.get(MAX_SIZE) + " byte. The url is " + url);
            }

            // Build file metadata map
            final Map<String, Object> fileMap = buildFileMap(file, content, size, url);

            final List<String> permissions = getFilePermissions(client, file);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            fileMap.put(FILE_ROLES, permissions);

            resultMap.put(FILE, fileMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("fileMap: {}", fileMap);
            }

            final String scriptType = getScriptType(paramMap);
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

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final Throwable t) {
            handleProcessingError(dataConfig, file, configMap, paramMap, dataMap, statsKey, crawlerStatsHelper, t);
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
     * Returns the permissions for a file.
     * @param client The GSuiteClient.
     * @param file The file.
     * @return The permissions for the file.
     */
    protected List<String> getFilePermissions(final GSuiteClient client, final File file) {
        final List<String> permissionList = new ArrayList<>();
        if (file.getPermissions() != null) {
            file.getPermissions().stream().map(this::getPermission).filter(s -> s != null).forEach(permissionList::add);
        }
        if (file.getOwners() != null) {
            file.getOwners().stream().map(this::getPermission).filter(s -> s != null).forEach(permissionList::add);
        }
        return permissionList;
    }

    /**
     * Returns the permission for a user.
     * @param user The user.
     * @return The permission for the user.
     */
    protected String getPermission(final User user) {
        if (logger.isDebugEnabled()) {
            logger.debug("user: {}", user);
        }
        return getPermission("user", user.getEmailAddress());
    }

    /**
     * Returns the permission for a permission.
     * @param permission The permission.
     * @return The permission for the permission.
     */
    protected String getPermission(final Permission permission) {
        if (logger.isDebugEnabled()) {
            logger.debug("permission: {}", permission);
        }
        if (Boolean.TRUE.equals(permission.getDeleted())) {
            return null;
        }
        return getPermission(permission.getType(), permission.getEmailAddress());
    }

    /**
     * Returns the permission for a type and value.
     * @param type The type.
     * @param value The value.
     * @return The permission for the type and value.
     */
    protected String getPermission(final String type, final String value) {
        if (value == null) {
            return null;
        }
        if ("user".equals(type)) {
            return ComponentUtil.getSystemHelper().getSearchRoleByUser(value);
        }
        if ("group".equals(type) || "domain".equals(type)) {
            return ComponentUtil.getSystemHelper().getSearchRoleByGroup(value);
        }
        if ("anyone".equals(type)) {
            return ComponentUtil.getSystemHelper().getSearchRoleByUser("guest");
        }
        return null;
    }

    /**
     * Returns the URL for a file.
     * @param configMap The configuration map.
     * @param paramMap The parameters for the data store.
     * @param file The file.
     * @return The URL for the file.
     */
    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final File file) {
        final String url = file.getWebContentLink();
        if (StringUtil.isBlank(url)) {
            final String id = file.getId();
            if (StringUtil.isNotBlank(id)) {
                return "https://drive.google.com/uc?id=" + id + "&export=download";
            }
            if (logger.isDebugEnabled()) {
                logger.debug("id is null.");
            }
        }
        return url;
    }

    /**
     * Returns the contents of a file.
     * Handles different file types appropriately:
     * - Google Docs/Presentations: exported as plain text
     * - Google Sheets: exported as CSV
     * - Google Apps Script: JSON parsed to extract script source code
     * - Other files: extracted using Tika extractor
     * @param client The GSuiteClient.
     * @param file The file.
     * @param ignoreError Whether to ignore errors.
     * @return The contents of the file.
     */
    protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
        final String mimeType = file.getMimeType();
        final String id = file.getId();

        // Check if this is a Google Apps file (e.g., application/vnd.google-apps.document)
        final Matcher m = GOOGLE_APPS_MIMETYPE_PATTERN.matcher(mimeType);
        if (m.matches()) {
            final String appType = m.group(1); // Extract the app type (e.g., "document", "spreadsheet")
            switch (appType) {
            case "document":
            case "presentation":
                // Export Google Docs and Presentations as plain text
                return client.extractFileText(id, "text/plain");
            case "spreadsheet":
                // Export Google Sheets as CSV format
                return client.extractFileText(id, "text/csv");
            case "script":
                // Google Apps Script files are exported as JSON
                // Parse the JSON to extract script file names and source code
                final String text = client.extractFileText(id, "application/vnd.google-apps.script+json");
                final StringBuilder sb = new StringBuilder();
                try {
                    final Map<String, Object> map = new ObjectMapper().readValue(text, new TypeReference<Map<String, Object>>() {
                    });
                    if (map.containsKey("files")) {
                        @SuppressWarnings("unchecked")
                        final List<Map<String, Object>> files = (List<Map<String, Object>>) map.get("files");
                        // Concatenate file names and their source code for indexing
                        files.forEach(f -> {
                            sb.append(f.getOrDefault("name", StringUtil.EMPTY));
                            sb.append("\n");
                            sb.append(f.getOrDefault("source", StringUtil.EMPTY));
                            sb.append("\n");
                        });
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to parse a json content.", e);
                }
                return sb.toString();
            default:
                // Other Google Apps file types (forms, drawings, etc.) are not explicitly handled
                break;
            }
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

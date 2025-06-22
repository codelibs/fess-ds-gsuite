/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

public class GoogleDriveDataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(GoogleDriveDataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    // parameters
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String URL_FILTER = "url_filter";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";

    // scripts
    protected static final String FILE = "file";
    protected static final String FILE_NAME = "name";
    protected static final String FILE_DESCRIPTION = "description";
    protected static final String FILE_CONTENTS = "contents";
    protected static final String FILE_MIMETYPE = "mimetype";
    protected static final String FILE_FILETYPE = "filetype";
    protected static final String FILE_THUMBNAIL_LINK = "thumbnail_link";
    protected static final String FILE_WEB_VIEW_LINK = "web_view_link";
    protected static final String FILE_WEB_CONTENT_LINK = "web_content_link";
    protected static final String FILE_CREATED_TIME = "created_time";
    protected static final String FILE_MODIFIED_TIME = "modified_time";
    protected static final String FILE_WRITERS_CAN_SHARE = "writers_can_share";
    protected static final String FILE_VIEWERS_CAN_COPY_CONTENT = "viewers_can_copy_content";
    protected static final String FILE_VIEWED_BY_ME_TIME = "viewed_by_me_time";
    protected static final String FILE_VIEWED_BY_ME = "viewed_by_me";
    protected static final String FILE_VIDEO_MEDIA_METADATA = "video_media_metadata";
    protected static final String FILE_VERSION = "version";
    protected static final String FILE_TRASHING_USER = "trashing_user";
    protected static final String FILE_TRASHED_TIME = "trashed_time";
    protected static final String FILE_TRASHED = "trashed";
    protected static final String FILE_THUMBNAIL_VERSION = "thumbnail_version";
    protected static final String FILE_TEAM_DRIVE_ID = "team_drive_id";
    protected static final String FILE_SHARED = "shared";
    protected static final String FILE_QUOTA_BYTES_USED = "quota_bytes_used";
    protected static final String FILE_PARENTS = "parents";
    protected static final String FILE_OWNERS = "owners";
    protected static final String FILE_OWNED_BY_ME = "owned_by_me";
    protected static final String FILE_ORIGINAL_FILENAME = "original_filename";
    protected static final String FILE_MODIFIED_BY_ME_TIME = "modified_by_me_time";
    protected static final String FILE_MODIFIED_BY_ME = "modified_by_me";
    protected static final String FILE_MD5_CHECKSUM = "md5_checksum";
    protected static final String FILE_LAST_MODIFIYING_USER = "last_modifiying_user";
    protected static final String FILE_KIND = "kind";
    protected static final String FILE_IS_APP_AUTHORIZED = "is_app_authorized";
    protected static final String FILE_IMAGE_MEDIA_METADATA = "image_media_metadata";
    protected static final String FILE_ID = "id";
    protected static final String FILE_ICON_LINK = "icon_link";
    protected static final String FILE_HEAD_REVISION_ID = "head_revision_id";
    protected static final String FILE_HAS_THUMBNAIL = "has_thumbnail";
    protected static final String FILE_HAS_ARGUMENTED_PERMISSIONS = "has_argumented_permissions";
    protected static final String FILE_FULL_FILE_EXTENSION = "full_file_extension";
    protected static final String FILE_FOLDER_COLOR_RBG = "folder_color_rbg";
    protected static final String FILE_FILE_EXTENSION = "file_extension";
    protected static final String FILE_EXPORT_LINKS = "export_links";
    protected static final String FILE_EXPLICITLY_TRASHED = "explicitly_trashed";
    protected static final String FILE_COPY_REQUIRES_WRITER_PERMISSION = "copy_requires_writer_permission";
    protected static final String FILE_APP_PROPERTIES = "app_properties";
    protected static final String FILE_CAPABILITIES = "capabilities";
    protected static final String FILE_CONTENT_HINTS = "content_hints";
    protected static final String FILE_CLASS_INFO = "class_info";
    protected static final String FILE_URL = "url";
    protected static final String FILE_SIZE = "size";
    protected static final String FILE_ROLES = "roles";

    protected String extractorName = "tikaExtractor";

    // other
    protected static final String FILE_FIELDS = "*";

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

    protected GSuiteClient createClient(final DataStoreParams paramMap) {
        return new GSuiteClient(paramMap);
    }

    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
    }

    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
    }

    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_SIZE);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
        } catch (final NumberFormatException e) {
            return DEFAULT_MAX_SIZE;
        }
    }

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

    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

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
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

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
            final String mimetype = file.getMimeType();
            if (((Boolean) configMap.get(IGNORE_FOLDER)).booleanValue() && "application/vnd.google-apps.folder".equals(mimetype)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignore item: {}", file.getWebContentLink());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);
            if (!Stream.of(supportedMimeTypes).anyMatch(mimetype::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", mimetype);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String url = getUrl(configMap, paramMap, file);
            final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            logger.info("Crawling URL: {}", url);

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> fileMap = new HashMap<>();

            final String content = getFileContents(client, file, ignoreError);
            final long size;
            if (file.getSize() != null) {
                size = file.getSize();
            } else if (content != null) {
                size = content.length();
            } else {
                size = 0;
            }

            if (size > ((Long) configMap.get(MAX_SIZE)).longValue()) {
                throw new MaxLengthExceededException(
                        "The content length (" + size + " byte) is over " + configMap.get(MAX_SIZE) + " byte. The url is " + url);
            }

            final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
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
            fileMap.put(FILE_FOLDER_COLOR_RBG, file.getFolderColorRgb());
            fileMap.put(FILE_FULL_FILE_EXTENSION, file.getFullFileExtension());
            fileMap.put(FILE_HAS_ARGUMENTED_PERMISSIONS, file.getHasAugmentedPermissions());
            fileMap.put(FILE_HAS_THUMBNAIL, file.getHasThumbnail());
            fileMap.put(FILE_HEAD_REVISION_ID, file.getHeadRevisionId());
            fileMap.put(FILE_ICON_LINK, file.getIconLink());
            fileMap.put(FILE_ID, file.getId());
            fileMap.put(FILE_IMAGE_MEDIA_METADATA, file.getImageMediaMetadata());
            fileMap.put(FILE_IS_APP_AUTHORIZED, file.getIsAppAuthorized());
            fileMap.put(FILE_KIND, file.getKind());
            fileMap.put(FILE_LAST_MODIFIYING_USER, file.getLastModifyingUser());
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
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
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
        } catch (final Throwable t) {
            String url = getUrl(configMap, paramMap, file);
            if (url == null) {
                url = StringUtil.EMPTY;
            }

            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    protected Date toDate(final com.google.api.client.util.DateTime date) {
        if (date == null) {
            return null;
        }
        return new Date(date.getValue());
    }

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

    protected String getPermission(final User user) {
        if (logger.isDebugEnabled()) {
            logger.debug("user: {}", user);
        }
        return getPermission("user", user.getEmailAddress());
    }

    protected String getPermission(final Permission permission) {
        if (logger.isDebugEnabled()) {
            logger.debug("permission: {}", permission);
        }
        if (Boolean.TRUE.equals(permission.getDeleted())) {
            return null;
        }
        return getPermission(permission.getType(), permission.getEmailAddress());
    }

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

    protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
        final String mimeType = file.getMimeType();
        final String id = file.getId();
        final Matcher m = Pattern.compile("application/vnd\\.google-apps\\.(.*)").matcher(mimeType);
        if (m.matches()) {
            switch (m.group(1)) {
            case "document":
            case "presentation":
                return client.extractFileText(id, "text/plain");
            case "spreadsheet":
                return client.extractFileText(id, "text/csv");
            case "script":
                final String text = client.extractFileText(id, "application/vnd.google-apps.script+json");
                final StringBuilder sb = new StringBuilder();
                try {
                    final Map<String, Object> map = new ObjectMapper().readValue(text, new TypeReference<Map<String, Object>>() {
                    });
                    if (map.containsKey("files")) {
                        @SuppressWarnings("unchecked")
                        final List<Map<String, Object>> files = (List<Map<String, Object>>) map.get("files");
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
                break;
            }
        }

        try (final InputStream in = client.getFileInputStream(id)) {
            return ComponentUtil.getExtractorFactory().builder(in, null).mimeType(mimeType).extractorName(extractorName).extract()
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

    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}

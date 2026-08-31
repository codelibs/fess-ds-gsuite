G Suite Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-gsuite/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-gsuite/actions/workflows/maven.yml)
==========================

## Overview

G Suite Data Store is an extension for Fess Data Store Crawling. It crawls Google Drive
and turns each file into a document.

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-gsuite/).

## Installation

1. Download fess-ds-gsuite-X.X.X.jar
2. Copy fess-ds-gsuite-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Breaking Changes in 15.9

A configuration written for 15.8 or earlier does not carry over unchanged. Read this
section before upgrading.

1. **`crawl_target` now defaults to `shared_drives`, and every target other than `legacy`
   requires `impersonate_user`.** An existing configuration that sets neither one fails at
   startup. Set `crawl_target=legacy` to keep the previous behaviour.
2. **The default OAuth scope is narrowed** to
   `https://www.googleapis.com/auth/drive.readonly`. Update the domain-wide delegation
   entry in the Google Workspace admin console to match, or set `scopes` explicitly.
3. **`crawl_target=users` and `crawl_target=both` additionally require**
   `https://www.googleapis.com/auth/admin.directory.user.readonly` in `scopes`. It is
   validated at startup.
4. **The indexed URL is now `webViewLink`** rather than `webContentLink`. Documents
   indexed by an earlier version keep their old URL, so re-crawl.
5. **`default_permissions` is a fallback, not an addition.** It applies only when the
   Drive ACL resolves to nothing; it is no longer merged into a non-empty ACL.
6. **Link-only sharing no longer grants a search role.** A `domain` or `anyone`
   permission with `allowFileDiscovery=false` is ignored.
7. **A document whose ACL resolves to nothing is skipped** rather than indexed with no
   role at all. Set `default_permissions` to index those documents instead.
8. **`fields` no longer defaults to `*`.** The default is an explicit projection, and a
   field outside it is null in the script context. Set `fields=*` to restore the previous
   behaviour.
9. **Google Docs are exported as Markdown and Google Sheets as TSV**, instead of plain
   text and CSV. Re-crawl to reindex the content.
10. **`refresh_token_interval` is ignored** and logs a warning. Access tokens are
    refreshed by google-auth-library.
11. **Google Forms and Google Sites are indexed as metadata only**, instead of producing
    a crawl error for each file.

## Getting Started

### Parameters

```
private_key=-----BEGIN PRIVATE KEY-----\n<BASE64 PRIVATE KEY>\n-----END PRIVATE KEY-----\n
private_key_id=<PRIVATE KEY ID>
client_email=<SERVICE ACCOUNT>@<PROJECT>.iam.gserviceaccount.com
impersonate_user=admin@example.com
crawl_target=shared_drives
```

The first three keys correspond to the keys of the JSON credentials of your service
account. `impersonate_user` is the Google Workspace admin account the service account
impersonates through domain-wide delegation; it is required unless `crawl_target=legacy`.

#### Authentication

| Parameter | Default | Description |
| --- | --- | --- |
| `private_key` | (required) | Private key of the service account, PEM with escaped newlines |
| `private_key_id` | (required) | Private key ID of the service account |
| `client_email` | (required) | Service account email address |
| `scopes` | `https://www.googleapis.com/auth/drive.readonly` | OAuth scopes, comma separated |
| `impersonate_user` | (none) | Admin account to impersonate. Required unless `crawl_target=legacy` |
| `refresh_token_interval` | (ignored) | Accepted but ignored; logs a warning |

#### Crawl scope

| Parameter | Default | Description |
| --- | --- | --- |
| `crawl_target` | `shared_drives` | `legacy`, `shared_drives`, `users` or `both` |
| `user_query` | (none) | Admin SDK `users.list` query. Used by `users` and `both` |
| `query` | (none) | Drive API search query |
| `corpora` | `allDrives` | Used by `crawl_target=legacy` only |
| `spaces` | (none) | Used by `crawl_target=legacy` and `users` only |
| `fields` | explicit projection | File field projection. Set `*` to request every field |

#### Access control

| Parameter | Default | Description |
| --- | --- | --- |
| `default_permissions` | (none) | Roles used when a file resolves to an empty ACL, comma separated |
| `domain_permission_format` | `{group}{domain}` | Role format applied to a `type=domain` permission |

#### Performance

| Parameter | Default | Description |
| --- | --- | --- |
| `number_of_threads` | `1` | Parallel file processing threads |
| `thread_pool_timeout_seconds` | `60` | Shutdown wait of the thread pool, in seconds |
| `page_size` | `1000` | Page size of `files.list` and `changes.list`, capped at 1000 |
| `permission_page_size` | `100` | Page size of `permissions.list` and `drives.list`, capped at 100 |
| `max_size` | `10000000` | Maximum file size to index, in bytes |
| `max_cached_content_size` | `1048576` | Content size kept in memory before spooling to disk, in bytes |

#### Retry

| Parameter | Default | Description |
| --- | --- | --- |
| `max_retries` | `5` | Retry budget for 429, quota 403 and 5xx responses |
| `retry_initial_interval_ms` | `1000` | Initial exponential back-off wait, in milliseconds |
| `max_backoff_ms` | `32000` | Upper bound of a single back-off wait, in milliseconds |

#### Network

| Parameter | Default | Description |
| --- | --- | --- |
| `read_timeout` | `20000` | HTTP read timeout, in milliseconds |
| `connect_timeout` | `20000` | HTTP connect timeout, in milliseconds |
| `proxy_host` | (none) | Proxy server host name |
| `proxy_port` | (none) | Proxy server port. Applied only together with `proxy_host` |
| `proxy_username` | (none) | Proxy user name |
| `proxy_password` | (none) | Proxy password |

#### Filtering

| Parameter | Default | Description |
| --- | --- | --- |
| `ignore_folder` | `true` | Skip folders |
| `ignore_error` | `true` | Continue the crawl when a file fails |
| `supported_mimetypes` | `.*` | MIME types to index, comma separated regexes |
| `include_pattern` | (none) | Regex of URLs to index |
| `exclude_pattern` | (none) | Regex of URLs to skip |

#### Incremental crawling

| Parameter | Default | Description |
| --- | --- | --- |
| `incremental` | `false` | Crawl only the Drive change feed |

An incremental crawl forces `delete_old_docs=false`, because a run that only sees changed
documents would otherwise sweep away every document it did not touch. The plugin writes
`start_page_tokens` and `crawl_signature` back into the parameter field to carry the
change feed position between runs; leave both to the plugin.

### Scripts

```
title=file.name
content=file.description+"\n"+file.contents
mimetype=file.mimetype
created=file.created_time
last_modified=file.modified_time
url=file.url
thumbnail=file.thumbnail_link
content_length=file.size
filetype=file.filetype
role=file.roles
filename=file.name
```

| Key | Value |
| --- | --- |
| file.name | The name of the file. |
| file.description | A short description of the file. |
| file.contents | The text contents of the file. |
| file.mimetype | The MIME type of the file. |
| file.filetype | The Fess file type of the file. |
| file.size | The size reported by Drive, or the extracted content length for a Google native type. |
| file.created_time | The time at which the file was created. |
| file.modified_time | The last time the file was modified by anyone. |
| file.url | The indexed URL of the file. |
| file.roles | The search roles resolved from the Drive ACL. |
| file.web_view_link | A link for opening the file in a relevant Google editor or viewer in a browser. |
| file.thumbnail_link | A short-lived link to the file's thumbnail, if available. Typically lasts on the order of hours. Only populated when the requesting app can access the file's content. |

The other Drive fields the connector maps are available under the same `file.` prefix, in
snake case, for example `file.owners`, `file.parents` and `file.md5_checksum`.

## Documentation

See [G Suite Data Store](https://fess.codelibs.org/15.9/config/datastore/ds-gsuite.html)
for the setup procedure and configuration examples.

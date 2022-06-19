G Suite Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-gsuite/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-gsuite/actions/workflows/maven.yml)
==========================

## Overview

G Suite Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-gsuite/).

## Installation

1. Download fess-ds-gsuite-X.X.X.jar
2. Copy fess-ds-gsuite-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Parameters

```
private_key=-----BEGIN PRIVATE KEY-----\nMIIEv...=\n-----END PRIVATE KEY-----\n
private_key_id=46812...b33f8
client_email=****@****.iam.gserviceaccount.com"
```

The keys correspond the keys of json including credentials of your service account.

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
| file.contents | The text contents of the File |
| file.mimetype | The MIME type of the file. |
| file.created_time | The time at which the file was created. |
| file.modified_time | The last time the file was modified by anyone. |
| file.web_view_link | A link for opening the file in a relevant Google editor or viewer in a browser. |
| file.thumbnail_link | A short-lived link to the file's thumbnail, if available. Typically lasts on the order of hours. Only populated when the requesting app can access the file's content. |

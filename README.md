G Suite Data Store for Fess [![Build Status](https://travis-ci.org/codelibs/fess-ds-gsuite.svg?branch=master)](https://travis-ci.org/codelibs/fess-ds-gsuite)
==========================

## Overview

G Suite Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](http://central.maven.org/maven2/org/codelibs/fess/fess-ds-gsuite/).

## Installation

1. Download fess-ds-gsuite-X.X.X.jar
2. Copy fess-ds-gsuite-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Parameters

```
project_id=****
private_key=-----BEGIN PRIVATE KEY-----\nMIIEv...=\n-----END PRIVATE KEY-----\n
private_key_id=46812...b33f8
client_email=****@****.iam.gserviceaccount.com"
```

The keys correspond the keys of json including credentials of your service account.

### Scripts

```
title=files.name
content=files.description+"\n"+files.contents
mimetype=files.mimetype
created=files.created_time
modified=files.modified_time
url=files.web_view_link
thumbnail=files.thumbnail_link
```

| Key | Value |
| --- | --- |
| files.name | The name of the file. |
| files.description | A short description of the file. |
| files.contents | The text contents of the File |
| files.mimetype | The MIME type of the file. |
| files.created_time | The time at which the file was created. |
| files.modified_time | The last time the file was modified by anyone. |
| files.web_view_link | A link for opening the file in a relevant Google editor or viewer in a browser. |
| files.thumbnail_link | A short-lived link to the file's thumbnail, if available. Typically lasts on the order of hours. Only populated when the requesting app can access the file's content. |
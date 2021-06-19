Box Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-box/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-box/actions/workflows/maven.yml)
==========================

## Overview

Box Data Store is an extension for Fess Data Store Crawling.

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-box/).

## Installation

1. Download fess-ds-box-X.X.X.jar
2. Copy fess-ds-box-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Parameters

```
client_id=hdf*****************************
client_secret=kMN**************************
public_key_id=4t******
private_key=-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFDj...=\n-----END ENCRYPTED PRIVATE KEY-----\n
passphrase=7ba*****************************
enterprise_id=19*******
```

### Scripts

```
url=file.url
title=file.name
content=file.contents
mimetype=file.mimetype
filetype=file.filetype
filename=file.name
content_length=file.size
created=file.created_at
last_modified=file.modified_at
```

| Key | Value |
| --- | --- |
| file.url | A link for opening the file in a browser. |
| file.contents | The text contents of the file |
| file.mimetype | The MIME type of the file |
| file.filetype | The file type of the file |

Please see [File Object](https://developer.box.com/reference#file-object)

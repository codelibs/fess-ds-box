Box Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-box/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-box/actions/workflows/maven.yml)
==========================

## Overview

Box Data Store is an extension for Fess Data Store Crawling. It authenticates as a Box app user
(JWT/server authentication), walks every enterprise user's files - or, with `root_folder_id`, a
single folder as the service account - and can map each file's Box collaborations onto Fess
search roles; see [Roles](#roles).

## Download

See [Maven Repository](https://maven.codelibs.org/release/org/codelibs/fess/fess-ds-box/).

## Installation

1. Download fess-ds-box-X.X.X.jar
2. Copy fess-ds-box-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Parameters

#### Authentication

```
client_id=hdf*****************************
client_secret=kMN**************************
public_key_id=4t******
private_key=-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFDj...=\n-----END ENCRYPTED PRIVATE KEY-----\n
passphrase=7ba*****************************
enterprise_id=19*******
```

| Parameter | Default | Description |
| --- | --- | --- |
| `client_id` | *(required)* | Client ID of the Box app used for JWT server authentication. |
| `client_secret` | *(required)* | Client secret of the Box app. |
| `public_key_id` | *(required)* | ID of the public key configured for the app's JWT keypair. |
| `private_key` | *(required)* | PEM-encoded private key for the JWT keypair. `\n` sequences in the value are unescaped to real newlines before use, so the key can be supplied on one line as shown above. |
| `passphrase` | *(required)* | Passphrase protecting `private_key`. |
| `enterprise_id` | *(required)* | Box enterprise ID the app authenticates against. |

All six are required together; the crawl fails at startup if any is blank. They - and
`proxy_password` below - are also stripped from the script evaluation context, so a crawling
script can never read a credential back.

#### Crawling

| Parameter | Default | Description |
| --- | --- | --- |
| `fields` | *(built-in list)* | Comma-separated Box API fields requested for each file, and used when listing a folder's children. Box returns only the fields it is explicitly asked for once a list is supplied, so an override must still cover everything the scripts below use. A folder's own document (see `ignore_folder`) always requests its own fixed, folder-appropriate field list, not this one. |
| `max_size` | `10000000` (~10MB) | Files larger than this many bytes are skipped rather than indexed. |
| `ignore_folder` | `true` | When `true`, only files are indexed, as before. When `false`, every descendant folder is indexed as its own document too - see [Behaviour changes](#behaviour-changes-in-this-release). |
| `ignore_error` | `true` | When `true`, a content-extraction failure indexes the document with empty content instead of failing the crawl. |
| `supported_mimetypes` | `.*` | Comma-separated regular expressions; a file whose resolved MIME type matches none of them is skipped. |
| `include_pattern` | *(none)* | Regular expression a file or folder's path must match to be indexed - see [Behaviour changes](#behaviour-changes-in-this-release) for what the path looks like. |
| `exclude_pattern` | *(none)* | Regular expression a file or folder's path must not match. |
| `number_of_threads` | `1` | Crawler worker threads, capped at twice the number of available processors. |
| `thread_pool_await_timeout` | `60` | Seconds to wait for queued work to finish once every user (or the single `root_folder_id` folder) has been queued, before force-shutting down the pool and dropping whatever is left. Values below `1` are raised to `1`. |
| `filter_term` | *(none)* | Term passed to Box's enterprise user listing, to crawl only a subset of users. Ignored when `root_folder_id` is set. |
| `root_folder_id` | *(none)* | Crawl a single folder as the service account instead of enumerating and impersonating every enterprise user. The service account needs its own access to the folder - typically by being added as a collaborator - because this path never impersonates a user. The folder itself is not indexed, only its contents, which matches how a user's own root folder is crawled. |
| `default_permissions` | *(none)* | Comma-separated Fess roles added to every document, in addition to whatever its Box collaborations resolve to; see [Roles](#roles). |
| `company_shared_link_role` | *(none)* | Fess role added to a document whose shared link is open to the whole enterprise ("Company" access level); see [Roles](#roles). |
| `readInterval` | `0` | Milliseconds to wait after queuing each file or folder, to throttle the crawl. |

#### Connection

| Parameter | Default | Description |
| --- | --- | --- |
| `base_url` | `https://app.box.com` | Used only to build the browsable link stored as `file.url`; it is not the Box API endpoint. |
| `refresh_token_interval` | `3540` (seconds) | How often the primary connection's access token is proactively refreshed in the background. |
| `proxy_host` / `proxy_port` | *(none)* | HTTP proxy for the Box connection. Both must be set to enable it. |
| `proxy_username` / `proxy_password` | *(none)* | Credentials for proxy basic authentication. Both must be set, and only take effect when `proxy_host`/`proxy_port` are also set. |
| `connect_timeout` | `0` | Connection timeout in milliseconds. `0` leaves the Box SDK's own default in place. |
| `read_timeout` | `0` | Read timeout in milliseconds. `0` leaves the Box SDK's own default in place. |
| `max_retry_attempts` | `5` | Retries the Box SDK performs itself for `429` and `5xx` responses, with backoff that honours `Retry-After`. A different layer from `max_retry_count` below. |
| `max_retry_count` | `10` | A separate, plugin-level retry loop that retries only `401` responses by rebuilding the connection. |

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
role=file.roles
```

| Key | Value |
| --- | --- |
| file.url | A link for opening the file in a browser. |
| file.contents | The text contents of the file |
| file.mimetype | The MIME type of the file |
| file.filetype | The file type of the file |
| file.roles | Fess search roles derived from the item's Box collaborations - files and, when `ignore_folder=false`, folders too - see [Roles](#roles). |

Please see [File Object](https://developer.box.com/reference#file-object)

### Roles

The normal way to turn Box's access control into Fess search permissions is `role=file.roles`:

```
default_permissions={role}admin
company_shared_link_role={role}employee
role=file.roles
```

`file.roles` merges the item's own accepted collaborations with those inherited from every
ancestor folder, the item's owner, `default_permissions` (added to every document regardless of
collaborations), and `company_shared_link_role` (added only when the item's shared link is set to
company-wide access). `pending` and `rejected` collaborations, and the `uploader` role, are never
included - see [Behaviour changes](#behaviour-changes-in-this-release).

`file.api.collaborationRoles` is an older, script-level form kept for backward compatibility. It
returns roles from the file's own collaborations only - not its ancestor folders, not
`default_permissions`, not `company_shared_link_role` - so it is a strict subset of what
`file.roles` produces. Prefer `role=file.roles` in new configurations.

**`file.api` exists on file documents only.** Calling `file.api.<anything>` in a script throws
when the document being processed is a folder, and the crawler records that as a failure-URL
entry for the folder. This only matters once you set `ignore_folder=false`: a script still using
`file.api` needs to guard the call - for example, only invoke it when `file.type == "file"` -
or every folder in the crawl will generate a failure-URL entry.

### Behaviour changes in this release

- **`pending` and `rejected` collaborations, and the `uploader` role, no longer grant search
  access.** `uploader` cannot preview or download the file in Box, so it never should have
  either. Some users may lose access to documents they could previously find through Fess; this
  is a correction to match Box's own access model, but it will look like a regression to them.
- **`include_pattern` and `exclude_pattern` now match against a path that ends with the item's
  own name**, for example `All Files/Projects/report.pdf` - relative, with no leading slash, and
  Box's root folder display name as the first segment. Previously the path stopped at the parent
  folder, so no pattern could ever select by file name or extension. Existing patterns may now
  match things they did not before.
- **A file shared with several collaborators is now crawled once**, instead of once per
  collaborator who can see it. The exception is a first pass that fails or produces a degraded
  document - a swallowed content-extraction failure, or a collaboration lookup the first user
  was not permitted to make. That pass releases its claim so a later, better-positioned
  collaborator can retry and overwrite the result.
- **`ignore_folder=false` now actually indexes folders.** It previously had no effect at all.

# Mailchimp output plugin for Embulk
[![Coverage Status](https://coveralls.io/repos/treasure-data/embulk-output-mailchimp/badge.svg?branch=master&service=github)](https://coveralls.io/github/treasure-data/embulk-output-mailchimp?branch=master)
[![Build Status](https://travis-ci.org/treasure-data/embulk-output-mailchimp.svg)](https://travis-ci.org/treasure-data/embulk-output-mailchimp?branch=master)

add e-mail to List in MailChimp.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **auth_method**: MailChimp auth method (string, `api_key` or `oauth`, default: `oauth`)
- **apikey**: MailChimp API key (string, required if `auth_method` is `api_key`)
- **access_token**: MailChimp access token (string, required if `auth_method` is `oauth`)
- **list_id**: MailChimp List id (string, required)
- **update_existing**: control whether to update members that are already subscribed to the list or to return an error (boolean, default: false)
- **merge_fields**: Array for merge fields/ TAG in MailChimp dashboard (array, optional, default: nil)
- **interest_categories**: Array for group names in MailChimp dashboard(array, default: nil)
- **double_optin**: Removed in API v3. Using `status` in json request body instead. Subscriber's current status: `subscribed`, `unsubscribed`, `cleaned`, `pending` 

## Example

```yaml
out:
  type: mailchimp
  auth_method: api_key
  apikey: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXX'
  list_id: 'XXXXXXXXXX'
  update_existing: false
  merge_fields:
  - FNAME
  - LNAME
  - WEB
  interest_categories:
  - Donating
  - Events
```

## Build

```
$ rake
```

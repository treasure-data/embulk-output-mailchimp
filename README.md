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

- **auth_method**: MailChimp auth method (string, `api_key` or `oauth`, default: `api_key`)
- **apikey**: MailChimp API key (string, required if `auth_method` is `api_key`)
- **access_token**: MailChimp access token (string, required if `auth_method` is `oauth`)
- **list_id**: MailChimp List id (string, required)
- **email_column**: column name for email (string, default: 'email')
- **fname_column**: column name for first name (string, default: 'fname')
- **lname_column**: column name for last name(string, default: 'lname')
- **update_existing**: control whether to update members that are already subscribed to the list or to return an error (boolean, default: false)
- **merge_fields**: Array for additional merge fields/ TAG in MailChimp dashboard (array, optional, default: nil)
- **grouping_columns**: Array for group names in MailChimp dashboard(array, default: nil)
- **double_optin**: control whether to send an opt-in confirmation email (boolean, default: true)

## Example

```yaml
out:
  type: mailchimp
  auth_method: api_key
  apikey: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXX'
  list_id: 'XXXXXXXXXX'
  update_existing: false
  double_optin: false
  email_column: e-mail
  fname_column: first name
  lname_column: lname
  merge_fields:
  - website
  grouping_columns:
  - interests
  - location
  replace_interests: true
```

## Build

```
$ rake
```

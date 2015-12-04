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

- **apikey**: Mailchimp API key (string, required)
- **list_id**: Mailchimp List id (string, required)
- **double_optin**: control whether to send an opt-in confirmation email (boolean, default: true)
- **update_existing**: control whether to update members that are already subscribed to the list or to return an error (boolean, default: false)
- **replace_interests**: determine whether we replace the interest groups with the updated groups provided, or we add the provided groups to the member's interest groups (boolean, default: true)
- **email_column**: column name for email (string, default: 'email')
- **fname_column**: column name for first name (string, default: 'fname')
- **lname_column**: column name for last name(string, default: 'lname')
- **grouping_columns**: column names array for group name(array, default: nil)

## Example

```yaml
out:
  type: mailchimp
  apikey: 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-XXX'
  list_id: 'XXXXXXXXXX'
  double_optin: false
  update_existing: false
  replace_interests: true
  email_column: 'e-mail'
  fname_column: 'first name'
  lname_column: 'lname'
  grouping_columns:
  - group_1
  - group_2
```


## Build

```
$ rake
```

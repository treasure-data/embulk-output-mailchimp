## 0.3.10 - 2017-06-10
- Fixed `MailChimpAbstractRecordBuffer` to move `cleanUp` method call in finally block [#25](https://github.com/treasure-data/embulk-output-mailchimp/pull/25)

## 0.3.9 - 2017-06-09
- Upgraded version to fix yanked gem file

## 0.3.8 - 2017-06-09
- Fixed JSON format in MERGE field's type is address [#24](https://github.com/treasure-data/embulk-output-mailchimp/pull/24)

## 0.3.7 - 2017-06-07
- Added `ConfigException` while using not exists `list_id` [#23](https://github.com/treasure-data/embulk-output-mailchimp/pull/23)

## 0.3.6 -2017-06-07
- Removed log query and add API to check address type [#22](https://github.com/treasure-data/embulk-output-mailchimp/pull/22)
- Enabled log query for logging on console temporary [#21](https://github.com/treasure-data/embulk-output-mailchimp/pull/21)

## 0.3.4 - 2017-06-01
- Enable JSON type for `address` MERGE field [#20](https://github.com/treasure-data/embulk-output-mailchimp/pull/20)

## 0.3.3 - 2017-05-31
- Enable multiple values for `interest categories` [#19](https://github.com/treasure-data/embulk-output-mailchimp/pull/19)

## 0.3.2 - 2017-05-30
- Rename `interest_categories` to `grouping_columns` to fix backward compatibility [#18](https://github.com/treasure-data/embulk-output-mailchimp/pull/18)

## 0.3.1 - 2017-05-26
- Enabled API v3 and supported OAuth2 beside API Key[#13](https://github.com/treasure-data/embulk-output-mailchimp/pull/13)
- Enabled double_optin in configuration and use default status if schema has no column `status` [#15](https://github.com/treasure-data/embulk-output-mailchimp/pull/15)
- Fixed bug can not extract data center when use `api_key` as `auth_mode` [#15](https://github.com/treasure-data/embulk-output-mailchimp/pull/15)
- Fixed compatible with API v2 and changed data format [#16](https://github.com/treasure-data/embulk-output-mailchimp/pull/16)
- Enable merge fields with case-insensitive [#17](https://github.com/treasure-data/embulk-output-mailchimp/pull/17)

## 0.2.3 - 2016-03-24

- Retry when Mailchimp server return Internal Server Error [#11](https://github.com/treasure-data/embulk-output-mailchimp/pull/11)

## 0.2.2 - 2016-01-21

- Changed the threshold of batch size for uploading emails as a request [#9](https://github.com/treasure-data/embulk-output-mailchimp/pull/9)

## 0.2.1 - 2016-01-06

- Logging API request/response [#5](https://github.com/treasure-data/embulk-output-mailchimp/pull/5)
- Default `double_optin` to be `false` [#6](https://github.com/treasure-data/embulk-output-mailchimp/pull/6)
- Fixed when non-string groups given [#7](https://github.com/treasure-data/embulk-output-mailchimp/pull/7)

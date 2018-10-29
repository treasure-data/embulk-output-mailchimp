## 0.3.26 - 2018-10-29

- Fix an NPE when there isn't a column corresponds to a configured group ('category')

## 0.3.25 - 2018-10-11

- Fixed an NPE when column names and merge tags are not exactly (case-sensitive) matched

## 0.3.23 - 2018-09-12
- Introduce an option to fail the job when there is an error returning from Mailchimp. Previous versions marked the job as success with
detail error in log
## 0.3.22 - 2018-03-07
- Fixed bug NPE when checking interest categories [#41](https://github.com/treasure-data/embulk-output-mailchimp/pull/41)

## 0.3.21 - 2018-03-02
- Refactor code to improve performance and fix NPE with merge fields that contain case sensitive [#40](https://github.com/treasure-data/embulk-output-mailchimp/pull/40)

## 0.3.20 - 2017-10-16
- Added pagination for interest categories [#39](https://github.com/treasure-data/embulk-output-mailchimp/pull/39)
- Refactor check list id to avoid confusing when get 404 error [#38](https://github.com/treasure-data/embulk-output-mailchimp/pull/38)
- Added pagination for merge fields [#37](https://github.com/treasure-data/embulk-output-mailchimp/pull/37)
- Fixed bug duplicated email in payload JSON [#36](https://github.com/treasure-data/embulk-output-mailchimp/pull/36)

## 0.3.19 - 2017-10-10
- Fixed bug can not parse invalid JSON response and added request timeout to avoid flush MailChimp API [#35](https://github.com/treasure-data/embulk-output-mailchimp/pull/35)

## 0.3.18 - 2017-09-19
- Fixed error parsing MailChimp API response JSON when push the large data [#34](https://github.com/treasure-data/embulk-output-mailchimp/pull/34)

## 0.3.17 - 2017-09-14

- Support language_column option [#32](https://github.com/treasure-data/embulk-output-mailchimp/pull/32) Thanks @gillax

## 0.3.16 - 2017-08-28
- Fixed user friendly error message when input invalid api key [#31](https://github.com/treasure-data/embulk-output-mailchimp/pull/31)

## 0.3.15 - 2017-07-26
- Add checking exception to retry [#30](https://github.com/treasure-data/embulk-output-mailchimp/pull/30)

## 0.3.14 - 2017-06-26
- Make clear the log message and renamed class `MailChimpClient` [#29](https://github.com/treasure-data/embulk-output-mailchimp/pull/29)

## 0.3.13 - 2017-06-16
- Upgraded `embulk-base-restclient` to v0.5.3 and fixed hang job [#28](https://github.com/treasure-data/embulk-output-mailchimp/pull/28)

## 0.3.12 - 2017-06-13
- Fixed NPE while reading JSON data [#27](https://github.com/treasure-data/embulk-output-mailchimp/pull/27)

## 0.3.11 - 2017-06-13
- Upgraded version of `embulk-base-restclient` to fix IndexOutOfBoundException [#26](https://github.com/treasure-data/embulk-output-mailchimp/pull/26)

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

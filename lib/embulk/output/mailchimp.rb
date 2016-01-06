require 'mailchimp'
require 'perfect_retry'

require 'securerandom'

module Embulk
  module Output

    class Mailchimp < OutputPlugin
      Plugin.register_output("mailchimp", self)

      class Client
        def initialize(apikey)
          @client = ::Mailchimp::API.new(apikey)
        rescue ::Mailchimp::InvalidApiKeyError => e
          raise Embulk::ConfigError.new(e.message)
        end

        def batch_subscribe_list(list_id, subscribers, double_optin, update_existing, replace_interests)
          @client.lists.batch_subscribe(list_id, subscribers, double_optin, update_existing, replace_interests)
        rescue ::Mailchimp::UserUnderMaintenanceError, ::Mailchimp::TooManyConnectionsError => e
          Embulk.logger.warn e.message
          raise e
        rescue ::Mailchimp::Error => e
          raise Embulk::DataError.new(e.message)
        end
      end

      MAX_EMAIL_COUNT = 1_000_000

      def self.transaction(config, schema, count, &control)
        task = {
          apikey:                 config.param("apikey",                 :string),
          list_id:                config.param("list_id",                :string),
          double_optin:           config.param("double_optin",           :bool,    default: false),
          update_existing:        config.param("update_existing",        :bool,    default: false),
          replace_interests:      config.param("replace_interests",      :bool,    default: true),
          email_column:           config.param("email_column",           :string,  default: "email"),
          fname_column:           config.param("fname_column",           :string,  default: "fname"),
          lname_column:           config.param("lname_column",           :string,  default: "lname"),
          grouping_columns:       config.param("grouping_columns",       :array,   default: nil),
          retry_limit:            config.param("retry_limit",            :integer, default: 5),
          retry_initial_wait_sec: config.param("retry_initial_wait_sec", :integer, default: 1),
          stop_on_invalid_record: config.param("stop_on_invalid_record", :bool,    default: true),
        }

        Client.new(task[:apikey]) # NOTE for validate apikey
        raise Embulk::ConfigError.new("schema has no '#{task[:email_column]}' column") if schema.none? {|s| s.name == task[:email_column] }

        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

      def init
        @client            = Client.new(task[:apikey])
        @list_id           = task[:list_id]
        @double_optin      = task[:double_optin]
        @update_existing   = task[:update_existing]
        @replace_interests = task[:replace_interests]
        @email_column      = task[:email_column]
        @fname_column      = task[:fname_column]
        @lname_column      = task[:lname_column]
        @grouping_columns  = task[:grouping_columns]
        @subscribers       = []

        @retry_manager = PerfectRetry.new do |config|
          config.limit = task[:retry_limit]
          config.sleep = lambda{|n| task[:retry_initial_wait_sec] * (2 ** (n - 1)) }
          config.logger = Embulk.logger
          config.log_level = nil
          config.dont_rescues = [Embulk::ConfigError, Embulk::DataError]
        end
        @stop_on_invalid_record = task[:stop_on_invalid_record]
      end

      def close
      end

      def add(page)
        # output code:
        page.each do |record|
          row = Hash[schema.names.zip(record)]
          if row[@email_column]
            add_subscriber row

            flush_subscribers! unless @subscribers.size < MAX_EMAIL_COUNT
          else
            raise Embulk::DataError.new("#{@email_column} is empty") if @stop_on_invalid_record
          end
        end
      end

      def finish
        flush_subscribers!
      end

      def abort
      end

      def commit
        task_report = {}
        return task_report
      end

      private

      # NOTE we expect row has #{email_column} column.
      #      expected full data is following columns.
      #  | #{email_column} | #{fname_column} | #{lname_column} | #{grouping_columns[0]} | #{grouping_columns[1]…|
      #  |-----------------|-----------------|-----------------|------------------------|-----------------------|
      #  | hi@example.com  | first_name      | last_name       | music,book             | middle                |
      def add_subscriber(row)
        return unless row[@email_column]

        merge_columns = {fname: @fname_column, lname: @lname_column}

        # NOTE merge_vars can add extra infomation.
        #      https://apidocs.mailchimp.com/api/2.0/lists/merge-vars.php
        #      embulk-output-mailchimp can add following infomation
        #        - lname
        #        - fname
        #        - interest group
        merge_vars = merge_columns.each_with_object({}) do |(key, col_name), m|
          m[key] = row[col_name] if row[col_name]
        end

        if @grouping_columns && !@grouping_columns.empty?
          merge_vars[:groupings] = []

          @grouping_columns.each do |group|
            next if row[group].nil? || row[group] == ''

            merge_vars[:groupings] << {
              'name'   => group,
              'groups' => row[group].split(','),
            }
          end
        end

        @subscribers << {
          EMAIL: {
            email: row[@email_column]
          },
          merge_vars: merge_vars,
        }
      end

      def flush_subscribers!
        return if @subscribers.empty?

        send_subscribers!
        @subscribers = []
      end

      def send_subscribers!
        @retry_manager.with_retry do
          request_id = SecureRandom.uuid
          Embulk.logger.info "[#{request_id}] Request : #{@subscribers.size} items"
          res = @client.batch_subscribe_list(
            @list_id,
            @subscribers,
            @double_optin,
            @update_existing,
            @replace_interests
          )
          Embulk.logger.info "[#{request_id}] Result : #{res.to_s}"
        end
      end
    end

  end
end

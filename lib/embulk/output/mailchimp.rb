require 'mailchimp'

module Embulk
  module Output

    class Mailchimp < OutputPlugin
      Plugin.register_output("mailchimp", self)

      MAX_EMAIL_COUNT = 1_000_000

      def self.transaction(config, schema, count, &control)
        task = {
          apikey:            config.param("apikey",            :string),
          list_id:           config.param("list_id",           :string),
          double_optin:      config.param("double_optin",      :bool,   default: true),
          update_existing:   config.param("update_existing",   :bool,   default: false),
          replace_interests: config.param("replace_interests", :bool,   default: true),
          email_column:      config.param("email_column",      :string, default: "email"),
          fname_column:      config.param("fname_column",      :string, default: "fname"),
          lname_column:      config.param("lname_column",      :string, default: "lname"),
        }

        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

      def init
        @client            = ::Mailchimp::API.new(task[:apikey])
        @list_id           = task[:list_id]
        @double_optin      = task[:double_optin]
        @update_existing   = task[:update_existing]
        @replace_interests = task[:replace_interests]
        @email_column      = task[:email_column]
        @fname_column      = task[:fname_column]
        @lname_column      = task[:lname_column]
        @subscribers       = []
      end

      def close
      end

      def add(page)
        # output code:
        page.each do |record|
          add_subscriber Hash[schema.names.zip(record)]
          flush_subscribers! unless @subscribers.size < MAX_EMAIL_COUNT
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

      def add_subscriber(row)
        return unless row[@email_column] # TODO raise ??

        merge_columns = {fname: @fname_column, lname: @lname_column}
        merge_vars = merge_columns.each_with_object({}) do |(key, col_name), m|
          m[key] = row[col_name] if row[col_name]
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

        @client.lists.batch_subscribe(
          @list_id,
          @subscribers,
          @double_optin,
          @update_existing,
          @replace_interests
        )

        @subscribers = []
      end
    end

  end
end

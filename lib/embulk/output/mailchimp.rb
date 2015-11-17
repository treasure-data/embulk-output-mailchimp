require 'mailchimp'

module Embulk
  module Output

    class Mailchimp < OutputPlugin
      Plugin.register_output("mailchimp", self)

      MAX_EMAIL_SIZE = 1_000_000

      def self.transaction(config, schema, count, &control)
        task = {
          apikey:       config.param("apikey", :string),
          list_id:      config.param("list_id", :string),
          double_optin: config.param("double_optin", :bool, default: true),
        }

        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

      def init
        @client       = ::Mailchimp::API.new(task[:apikey])
        @list_id      = task[:list_id]
        @double_optin = task[:double_optin]
        @subscribers  = []
      end

      def close
      end

      def add(page)
        # output code:
        page.each do |record|
          add_subscriber Hash[schema.names.zip(record)]
          flush_subscribers! unless @subscribers.size < MAX_EMAIL_SIZE
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
        return unless row['email'] # TODO raise ??

        merge_vars = %w(lname fname).each_with_object({}) do |col_name, m|
          m[col_name] = row[col_name] if row[col_name]
        end

        @subscribers << {
          EMAIL: {
            email: row['email']
          },
          merge_vars: merge_vars,
        }
      end

      def flush_subscribers!
        @client.lists.batch_subscribe(
          @list_id,
          @subscribers,
          @double_optin
        )

        @subscribers = []
      end
    end

  end
end

require "embulk"
Embulk.setup

require "embulk/output/mailchimp"
require "override_assert_raise"


module Embulk
  module Output
    class TestMailchimp < Test::Unit::TestCase
      include OverrideAssertRaise

      def apikey
        'XXXXXXXXXXXXXXXXXXXXXX'
      end

      def list_id
        'listlist'
      end

      class TestTransaction < self
        def base_config
          {
            type: 'mailchimp',
            apikey: apikey,
            list_id: list_id,
          }
        end

        class MailchimpAPIKeyIsValid < self
          def setup
            stub(Embulk::Output::Mailchimp::Client).new(apikey) { }
          end

          def test_call_finish_without_error
            source = create_config(base_config)
            schema = create_schema([{name: 'email', type: :string}])

            Embulk::Output::Mailchimp.transaction(source, schema, 1) {}
          end

          def test_schema_has_no_email_column
            source = create_config(base_config.merge(email_column: 'e_mail'))
            schema = create_schema([{name: 'email', type: :string}])

            assert_raise(Embulk::ConfigError) do
              Embulk::Output::Mailchimp.transaction(source, schema, 1) {}
            end
          end
        end

        class MailchimpAPIKeyIsInvalid < self
          def setup
            stub(::Mailchimp::API).new(apikey) { raise ::Mailchimp::InvalidApiKeyError }
          end

          def test_call_finish_without_error
            source = create_config(base_config)
            schema = create_schema([{name: 'email', type: :string}])

            assert_raise(Embulk::ConfigError) do
              Embulk::Output::Mailchimp.transaction(source, schema, 1) {}
            end
          end
        end
      end

      class TestFinish < self
        setup do
          stub(::Mailchimp::API).new(apikey) { }

          @plugin = Mailchimp.new(
            {
              apikey: apikey,
              list_id: list_id,
              email_column: 'email',
              retry_limit: retry_limit,
              retry_initial_wait_sec: 0,
            },
            create_schema([
              {name: 'email', type: :string},
            ]),
            1
          )
        end

        def retry_limit
          3
        end

        def test_retry_on_mailchimp_error
          client = @plugin.instance_variable_get(:@client)
          stub(client).batch_subscribe_list(list_id, anything, anything, anything, anything) {
            # https://bitbucket.org/mailchimp/mailchimp-api-ruby/src/37dbe82057b96e135881c9379c0a3d5f8f32bf1f/lib/mailchimp.rb?at=master&fileviewer=file-view-default#mailchimp.rb-164
            raise ::Mailchimp::Error, "We received an unexpected error: <!doctype html>"
          }
          stub(Embulk.logger).info(anything)
          mock(Embulk.logger).warn(anything).at_least(retry_limit)

          @plugin.add([
            ["foo@example.com"]
          ])
          assert_raise(PerfectRetry::TooManyRetry) do
            @plugin.finish
          end
        end
      end

      class TestAdd < self
        setup do
          stub(::Mailchimp::API).new(apikey) { }
        end

        def create_mailchimp(optional_task = {})
          Mailchimp.new(
            {apikey: apikey, list_id: list_id, email_column: 'email'}.merge(optional_task),
            create_schema([
              {name: 'email', type: :string},
              {name: 'fname', type: :string},
              {name: 'lname', type: :string},
              {name: 'list_group1', type: :string},
              {name: 'list_group2', type: :string},
            ]),
            1
          )
        end

        class MergeVars < self
          def page
            [['a@example.com', 'first', 'last']]
          end

          def page_with_groupings
            [['a@example.com', 'first', 'last', 'group_1,group_2', 'group_3,group_4']]
          end

          def test_stop_on_invalid_record_is_false
            mailchimp = create_mailchimp(
              lname_column: 'lname',
              fname_column: 'fname'
            )

            mailchimp.add(page)
            subscriber = mailchimp.instance_variable_get(:@subscribers)[0]

            assert_equal(subscriber[:EMAIL][:email], 'a@example.com')
            assert_equal(subscriber[:merge_vars][:fname], 'first')
            assert_equal(subscriber[:merge_vars][:lname], 'last')
          end

          def test_grouping_columns
            mailchimp = create_mailchimp(
              lname_column: 'lname',
              fname_column: 'fname',
              grouping_columns: ['list_group1', 'list_group2']
            )

            mailchimp.add(page_with_groupings)
            subscriber = mailchimp.instance_variable_get(:@subscribers)[0]

            assert_equal(
              subscriber[:merge_vars][:groupings],
              [
                {'name' => 'list_group1', 'groups' => %w(group_1 group_2)},
                {'name' => 'list_group2', 'groups' => %w(group_3 group_4)},
              ]
            )
          end

          def test_grouping_columns_not_string
            groups_1 = 42
            groups_2 = 1.3

            mailchimp = create_mailchimp(
              lname_column: 'lname',
              fname_column: 'fname',
              grouping_columns: ['list_group1', 'list_group2']
            )

            mailchimp.add([['i@example.com', 'lname', 'fname', groups_1, groups_2]])
            subscriber = mailchimp.instance_variable_get(:@subscribers)[0]

            assert_equal(
              subscriber[:merge_vars][:groupings],
              [
                {'name' => 'list_group1', 'groups' => [groups_1.to_s] },
                {'name' => 'list_group2', 'groups' => [groups_2.to_s] },
              ]
            )
          end
        end

        # NOTE skip until const mocking
        class RecordeOverMaxEmailCount# < self
          def page
            [['@example.com']] * Mailchimp::MAX_EMAIL_COUNT
          end

          def test_flush_if_over_max_email_count
            mailchimp = create_mailchimp
            mock(mailchimp).send_subscribers!

            mailchimp.add(page)
            assert_equal(mailchimp.instance_variable_get(:@subscribers).size, 1)
          end
        end

        class EmailColumnValueIsEmpty < self
          def page
            [
              ['a@example.com'], [], ['c@example.com']
            ]
          end

          def test_stop_on_invalid_record_is_false
            mailchimp = create_mailchimp(stop_on_invalid_record: false)

            mailchimp.add(page)
            assert_equal(mailchimp.instance_variable_get(:@subscribers).size, 2)
          end

          def test_stop_on_invalid_record_is_true
            mailchimp = create_mailchimp(stop_on_invalid_record: true)

            assert_raise(Embulk::DataError) do
              mailchimp.add(page)
            end
          end
        end
      end

      private

      def create_config(config)
        Embulk::DataSource[*config.to_a.flatten(1)]
      end

      def create_schema(columns)
        Embulk::Schema.new(columns.map {|c| Embulk::Column.new(c) })
      end

      def mute_logger
        stub(Embulk).logger { Logger.new(File::NULL) }
      end

      def setup_plugin(specific_task = nil, specific_schema = nil)
        @plugin = Mailchimp.new(specific_task || task, specific_schema || schema, 1)
        stub(@plugin).task { specific_task || task }
        @plugin.init
      end
    end
  end
end

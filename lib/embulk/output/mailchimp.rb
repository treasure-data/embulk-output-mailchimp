Embulk::JavaPlugin.register_output(
  "mailchimp", "org.embulk.output.mailchimp.MailChimpOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
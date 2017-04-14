package org.embulk.output.mailchimp;

import com.google.common.base.Throwables;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;

import java.util.List;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailchimpOutputPluginDelegate
        implements RestClientOutputPluginDelegate<MailchimpOutputPluginDelegate.PluginTask>
{
    private static final Logger LOG = Exec.getLogger(MailchimpOutputPluginDelegate.class);

    public MailchimpOutputPluginDelegate()
    {
    }

    public interface PluginTask
            extends RestClientOutputTaskBase
    {
        @Config("maximum_retries")
        @ConfigDefault("6")
        int getMaximumRetries();

        @Config("initial_retry_interval_millis")
        @ConfigDefault("1000")
        int getInitialRetryIntervalMillis();

        @Config("maximum_retry_interval_millis")
        @ConfigDefault("32000")
        int getMaximumRetryIntervalMillis();

        @Config("timeout_millis")
        @ConfigDefault("60000")
        int getTimeoutMills();

        @Config("client_id")
        String getClientId();

        @Config("client_secret")
        String getClientSecret();

        @Config("refresh_token")
        String getRefreshToken();

        @Config("list_id")
        String getListId();

        @Config("double_optin")
        @ConfigDefault("false")
        boolean isDoubleOptIn();

        @Config("update_existing")
        @ConfigDefault("false")
        boolean isUpdateExisting();

        @Config("replace_interests")
        @ConfigDefault("false")
        boolean isReplaceInterests();

//        @Config("email_column")
//        @ConfigDefault("null")
//        Optional<String> getEmailColumn();
//
//        @Config("fname_column")
//        @ConfigDefault("null")
//        Optional<String> getFirstNameColumn();
//
//        @Config("lname_column")
//        @ConfigDefault("null")
//        Optional<String> getLastNameColumn();
//
//        @Config("grouping_columns")
//        @ConfigDefault("null")
//        List<String> getGroupingColumns();
    }

    @Override
    public void validateOutputTask(final PluginTask task, final Schema schema, final int taskCount)
    {

    }

    @Override
    public RecordBuffer buildRecordBuffer(final PluginTask task)
    {
        Jetty92RetryHelper retryHelper = createRetryHelper(task);
        return new MailchimpRecordBuffer("records", task, retryHelper);
    }

    @Override
    public JacksonServiceRequestMapper buildServiceRequestMapper(final PluginTask task)
    {
        return JacksonServiceRequestMapper.builder()
                .add(new JacksonAllInObjectScope(), new JacksonTopLevelValueLocator("record"))
                .build();
    }

    @Override
    public ConfigDiff egestEmbulkData(final PluginTask task, final Schema schema, final int taskCount,
                                      final List<TaskReport> taskReports)
    {
        long totalInserted = 0;
        for (TaskReport taskReport : taskReports) {
            if (taskReport.has("inserted")) {
                totalInserted += taskReport.get(Long.class, "inserted");
            }
        }

        LOG.info("Insert completed. {} records", totalInserted);

        return Exec.newConfigDiff();
    }

    protected Jetty92RetryHelper createRetryHelper(PluginTask task)
    {
        return new Jetty92RetryHelper(
                task.getMaximumRetries(),
                task.getInitialRetryIntervalMillis(),
                task.getMaximumRetryIntervalMillis(),
                new Jetty92ClientCreator()
                {
                    @Override
                    public HttpClient createAndStart()
                    {
                        HttpClient client = new HttpClient(new SslContextFactory());
                        try {
                            client.start();
                            return client;
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }
}

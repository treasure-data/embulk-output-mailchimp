package org.embulk.output.mailchimp;

import com.google.common.base.Optional;
import org.embulk.base.restclient.RestClientOutputPluginDelegate;
import org.embulk.base.restclient.RestClientOutputTaskBase;
import org.embulk.base.restclient.jackson.JacksonServiceRequestMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.jackson.scope.JacksonAllInObjectScope;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.mailchimp.model.AuthMethod;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.embulk.output.mailchimp.validation.ColumnDataValidator.checkExistColumns;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailChimpOutputPluginDelegate
        implements RestClientOutputPluginDelegate<MailChimpOutputPluginDelegate.PluginTask>
{
    private static final Logger LOG = Exec.getLogger(MailChimpOutputPluginDelegate.class);

    public MailChimpOutputPluginDelegate()
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
        int getTimeoutMillis();

        @Config("auth_method")
        @ConfigDefault("api_key")
        AuthMethod getAuthMethod();

        @Config("apikey")
        @ConfigDefault("null")
        Optional<String> getApikey();

        @Config("access_token")
        @ConfigDefault("null")
        Optional<String> getAccessToken();

        @Config("list_id")
        String getListId();

        @Config("email_column")
        @ConfigDefault("email")
        String getEmailColumn();

        @Config("fname_column")
        @ConfigDefault("fname")
        String getFnameColumn();

        @Config("lname_column")
        @ConfigDefault("lname")
        String getLnameColumn();

        @Config("merge_fields")
        @ConfigDefault("null")
        Optional<List<String>> getMergeFields();

        @Config("grouping_columns")
        @ConfigDefault("null")
        Optional<List<String>> getGroupingColumns();

        @Config("double_optin")
        @ConfigDefault("true")
        boolean getDoubleOptIn();

        @Config("update_existing")
        @ConfigDefault("false")
        boolean getUpdateExisting();

        @Config("replace_interests")
        @ConfigDefault("true")
        boolean getReplaceInterests();

        @Config("language_column")
        @ConfigDefault("null")
        Optional<String> getLanguageColumn();
    }

    /**
     * Override @{@link RestClientOutputPluginDelegate#validateOutputTask(RestClientOutputTaskBase, Schema, int)}
     * This method not only validates required configurations but also validates required columns
     *
     * @param task
     * @param schema
     * @param taskCount
     */
    @Override
    public void validateOutputTask(final PluginTask task, final Schema schema, final int taskCount)
    {
        if (task.getAuthMethod() == AuthMethod.OAUTH) {
            if (!task.getAccessToken().isPresent() || isNullOrEmpty(task.getAccessToken().get())) {
                throw new ConfigException("'access_token' is required when auth_method is 'oauth'");
            }
        }
        else if (task.getAuthMethod() == AuthMethod.API_KEY) {
            if (!task.getApikey().isPresent() || isNullOrEmpty(task.getApikey().get())) {
                throw new ConfigException("'apikey' is required when auth_method is 'api_key'");
            }
        }

        if (isNullOrEmpty(task.getListId())) {
            throw new ConfigException("'list_id' must not be null or empty string");
        }

        if (!checkExistColumns(schema, task.getEmailColumn(), task.getFnameColumn(), task.getLnameColumn())) {
            throw new ConfigException("Columns ['email', 'fname', 'lname'] must not be null or empty string");
        }
    }

    @Override
    public RecordBuffer buildRecordBuffer(PluginTask task, Schema schema, int taskIndex)
    {
        return new MailChimpRecordBuffer(schema, task);
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
            if (taskReport.has("pushed")) {
                totalInserted += taskReport.get(Long.class, "pushed");
            }
        }

        LOG.info("Pushed completed. {} records", totalInserted);

        return Exec.newConfigDiff();
    }
}

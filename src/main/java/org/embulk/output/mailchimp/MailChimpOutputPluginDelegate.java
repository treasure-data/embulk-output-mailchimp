package org.embulk.output.mailchimp;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.caseInsensitiveColumnNames;
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
        @Config("retry_limit")
        @ConfigDefault("6")
        int getRetryLimit();

        @Config("retry_initial_wait_msec")
        @ConfigDefault("1000")
        int getRetryInitialWaitMSec();

        @Config("max_retry_wait_msec")
        @ConfigDefault("32000")
        int getMaxRetryWaitMSec();

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

        @Config("atomic_upsert")
        @ConfigDefault("false")
        boolean getAtomicUpsert();

        @Config("replace_interests")
        @ConfigDefault("true")
        boolean getReplaceInterests();

        @Config("language_column")
        @ConfigDefault("null")
        Optional<String> getLanguageColumn();

        @Config("max_records_per_request")
        @ConfigDefault("500")
        int getMaxRecordsPerRequest();

        @Config("sleep_between_requests_millis")
        @ConfigDefault("3000")
        int getSleepBetweenRequestsMillis();
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

            if (!task.getApikey().get().contains("-")) {
                throw new ConfigException("apikey's format invalid.");
            }
        }

        if (isNullOrEmpty(task.getListId())) {
            throw new ConfigException("'list_id' must not be null or empty string");
        }

        if (!checkExistColumns(schema, task.getEmailColumn(), task.getFnameColumn(), task.getLnameColumn())) {
            throw new ConfigException("Columns ['email', 'fname', 'lname'] must not be null or empty string");
        }
        if (task.getAtomicUpsert()) {
            LOG.info(" Treating upsert as atomic operation");
        }

        // Warn if schema doesn't have the task's grouping column (Group Category)
        if (task.getGroupingColumns().isPresent()
                && !task.getGroupingColumns().get().isEmpty()) {
            Set<String> categoryNames = new HashSet<>(task.getGroupingColumns().get());
            categoryNames.removeAll(caseInsensitiveColumnNames(schema));
            if (categoryNames.size() > 0) {
                LOG.warn("Data schema doesn't contain the task's grouping column(s): {}", on(", ").join(categoryNames));
            }
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
        int totalError = 0;
        for (TaskReport taskReport : taskReports) {
            if (taskReport.has("pushed")) {
                totalInserted += taskReport.get(Long.class, "pushed");
            }
            if (taskReport.has("error_count")) {
                totalError += taskReport.get(Integer.class, "error_count");
            }
        }
        LOG.info("Pushed completed. {} records", totalInserted);
        // When atomic upsert is true, client expects all records are done properly.
        if (task.getAtomicUpsert() && totalError > 0) {
            LOG.info("Job requires atomic operation for all records. And there were {} errors in processing => Error as job's status", totalError);
            throw Throwables.propagate(new DataException("Some records are not properly processed at MailChimp. See log for detail"));
        }
        return Exec.newConfigDiff();
    }
}

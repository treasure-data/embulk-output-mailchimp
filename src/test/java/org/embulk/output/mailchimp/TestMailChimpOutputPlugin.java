package org.embulk.output.mailchimp;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.mailchimp.test.EmbulkTestsWithGuava;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.List;

import static org.embulk.output.mailchimp.CircleCICredentials.credentials;

/**
 * Created by thangnc on 4/14/17.
 */
public class TestMailChimpOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource baseConfig;
    private MailChimpOutputPluginDelegate.PluginTask task;
    private MailChimpOutputPlugin plugin;
    private String resourcesPath;

    @Before
    public void setup()
    {
        plugin = new MailChimpOutputPlugin();
        baseConfig = EmbulkTestsWithGuava.config("EMBULK_OUTPUT_MAILCHIMP_TEST_CONFIG").merge(credentials());
        task = baseConfig.loadConfig(MailChimpOutputPluginDelegate.PluginTask.class);
        URL csvFilePath = getClass().getClassLoader().getResource("csv");
        if (csvFilePath != null) {
            resourcesPath = new File(csvFilePath.getFile()).getAbsolutePath();
        }
    }

    @After
    public void teardown()
    {
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithEmptyApiKey()
    {
        ConfigSource config = baseConfig.set("apikey", "");
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithNullApiKey()
    {
        ConfigSource config = baseConfig.set("apikey", null);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithEmptyAccessToken()
    {
        ConfigSource config = baseConfig.set("auth_method", "oauth")
                .set("access_token", "");
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithNullAccessToken()
    {
        ConfigSource config = baseConfig.set("auth_method", "oauth")
                .set("access_token", null);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithNullListId()
    {
        ConfigSource config = baseConfig.set("list_id", null);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithEmptyListId()
    {
        ConfigSource config = baseConfig.set("list_id", "");
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithColumnEmailRequires()
    {
        ConfigSource config = baseConfig;
        Schema schema = Schema.builder()
                .add("firstname", org.embulk.spi.type.Types.STRING)
                .add("lastname", org.embulk.spi.type.Types.STRING)
                .add("status", org.embulk.spi.type.Types.STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithColumnStatusRequires()
    {
        ConfigSource config = baseConfig;
        Schema schema = Schema.builder()
                .add("email", org.embulk.spi.type.Types.STRING)
                .add("fname", org.embulk.spi.type.Types.STRING)
                .add("lname", org.embulk.spi.type.Types.STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }

    private class OutputControl implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(Exec.newTaskReport());
        }
    }

    private void doSetUpSchemaAndRun(final ConfigSource config, final MailChimpOutputPlugin plugin)
    {
        Schema schema = Schema.builder()
                .add("email", org.embulk.spi.type.Types.STRING)
                .add("fname", org.embulk.spi.type.Types.STRING)
                .add("lname", org.embulk.spi.type.Types.STRING)
                .add("status", org.embulk.spi.type.Types.STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }
}

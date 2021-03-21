package org.embulk.output.mailchimp;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.embulk.output.mailchimp.MailChimpOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.embulk.spi.type.Types.STRING;

/**
 * Created by thangnc on 4/14/17.
 */
public class TestMailChimpOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    private ConfigSource baseConfig;
    private MailChimpOutputPluginDelegate.PluginTask task;
    private MailChimpOutputPlugin plugin;

    @Before
    public void setup()
    {
        plugin = new MailChimpOutputPlugin();
        baseConfig = config();
        task = CONFIG_MAPPER.map(baseConfig, MailChimpOutputPluginDelegate.PluginTask.class);
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
    public void test_config_atomicUpsert()
    {
        ConfigSource config = baseConfig.set("atomic_upsert", true);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_config_invalidWithColumnEmailRequires()
    {
        ConfigSource config = baseConfig;
        Schema schema = Schema.builder()
                .add("fname", STRING)
                .add("lname", STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }

    /**
     * Load plugin config with Guava & Joda support
     */
    private static ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "mailchimp")
                .set("auth_method", "api_key")
                .set("apikey", "xxxxxxxxxxxxxxxxxxx")
                .set("access_token", "xxxxxxxxxxxxxxxxxxx")
                .set("list_id", "xxxxxxxxxxxxxxxxxxx")
                .set("email_column", "email")
                .set("fname_column", "fname")
                .set("lname_column", "lname");
    }

    private static class OutputControl implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(CONFIG_MAPPER_FACTORY.newTaskReport());
        }
    }

    private void doSetUpSchemaAndRun(final ConfigSource config, final MailChimpOutputPlugin plugin)
    {
        Schema schema = Schema.builder()
                .add("email", STRING)
                .add("fname", STRING)
                .add("lname", STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.toTaskSource(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }
}

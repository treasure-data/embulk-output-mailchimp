package org.embulk.output.mailchimp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.embulk.spi.type.Types.STRING;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

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

    @Before
    public void setup()
    {
        plugin = new MailChimpOutputPlugin();
        baseConfig = config();
        task = baseConfig.loadConfig(MailChimpOutputPluginDelegate.PluginTask.class);
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

        final TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }

    /**
     * Load plugin config with Guava & Joda support
     */
    private static ConfigSource config()
    {
        String path = System.getenv("EMBULK_OUTPUT_MAILCHIMP_TEST_CONFIG");
        assumeThat(isNullOrEmpty(path), is(false));
        try {
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new GuavaModule())
                    .registerModule(new JodaModule());
            ConfigLoader configLoader = new ConfigLoader(new ModelManager(null, mapper));
            return configLoader.fromYamlFile(new File(path));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
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
                .add("email", STRING)
                .add("fname", STRING)
                .add("lname", STRING)
                .build();

        final TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.finish();

        plugin.transaction(config, schema, 0, new OutputControl());
    }
}

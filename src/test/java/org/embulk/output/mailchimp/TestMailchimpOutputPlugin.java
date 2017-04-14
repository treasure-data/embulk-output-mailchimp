package org.embulk.output.mailchimp;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.mailchimp.test.EmbulkTestsWithGuava;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.embulk.output.mailchimp.CircleCICredentials.credentials;
import static org.embulk.output.mailchimp.test.TestUtils.doSetUpSchemaAndRun;
import static org.embulk.output.mailchimp.test.TestUtils.existsConfig;

/**
 * Created by thangnc on 4/14/17.
 */
public class TestMailchimpOutputPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource baseConfig;
    private MailchimpOutputPluginDelegate.PluginTask task;
    private MailchimpOutputPlugin plugin;
    private String resourcesPath;

    @Before
    public void setup()
    {
        plugin = new MailchimpOutputPlugin();
        baseConfig = EmbulkTestsWithGuava.config("EMBULK_OUTPUT_MAILCHIMP_TEST_CONFIG").merge(credentials());
        task = baseConfig.loadConfig(MailchimpOutputPluginDelegate.PluginTask.class);
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
    public void test_transaction_hasInvalidWithEmptyClientId()
    {
        ConfigSource config = existsConfig(resourcesPath,
                                           baseConfig)
                .set("client_secret", null);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_transaction_hasInvalidWithNullClientId()
    {
        ConfigSource config = existsConfig(resourcesPath,
                                           baseConfig)
                .set("client_id", null);
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_transaction_hasInvalidWithEmptyClientSecret()
    {
        ConfigSource config = existsConfig(resourcesPath,
                                           baseConfig)
                .set("client_secret", "");
        doSetUpSchemaAndRun(config, plugin);
    }

    @Test(expected = ConfigException.class)
    public void test_transaction_hasInvalidWithNullClientSecret()
    {
        ConfigSource config = existsConfig(resourcesPath,
                                           baseConfig)
                .set("client_secret", null);
        doSetUpSchemaAndRun(config, plugin);
    }
}

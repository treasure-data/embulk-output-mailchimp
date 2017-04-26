package org.embulk.output.mailchimp;

import com.fasterxml.jackson.databind.JsonNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.output.mailchimp.test.EmbulkTestsWithGuava;
import org.embulk.spi.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;

import static org.embulk.output.mailchimp.CircleCICredentials.credentials;

/**
 * Created by thangnc on 4/26/17.
 */
public class TestMailChimpRecordBuffer
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

    @Test
    public void test_processSubscribers_valid()
    {
        Schema schema = Schema.builder()
                .add("email", org.embulk.spi.type.Types.STRING)
                .add("firstname", org.embulk.spi.type.Types.STRING)
                .add("lastname", org.embulk.spi.type.Types.STRING)
                .add("status", org.embulk.spi.type.Types.STRING)
                .build();

        MailChimpRecordBuffer recordBuffer = new MailChimpRecordBuffer(schema, task);
        recordBuffer.processSubcribers(new ArrayList<JsonNode>(), task);
    }
}

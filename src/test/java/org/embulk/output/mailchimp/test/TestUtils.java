package org.embulk.output.mailchimp.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.mailchimp.MailchimpOutputPlugin;
import org.embulk.output.mailchimp.MailchimpOutputPluginDelegate;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.standards.CsvParserPlugin;

import java.util.List;

/**
 * Created by thangnc on 1/26/17.
 */
public final class TestUtils
{
    private TestUtils()
    {
    }

    /**
     * Initialize input source configuration parameters. The configuration
     * parameters belongs to file input plugin of TD
     *
     * @param resourcesPath the resources path
     * @return Immutable Map of configuration parameter name and value
     */
    private static ImmutableMap<String, Object> inputConfig(final String resourcesPath)
    {
        return new ImmutableMap.Builder<String, Object>()
                .put("type", "file")
                .put("path_prefix", resourcesPath)
                .put("last_path", "")
                .build();
    }

    private static ImmutableList<Object> schemaConfig()
    {
        return new ImmutableList.Builder<>()
                .add(ImmutableMap.of("name", "email", "type", "string"))
                .add(ImmutableMap.of("name", "firstname", "type", "string"))
                .add(ImmutableMap.of("name", "lastname", "type", "string"))
                .build();
    }

    /**
     * Configure parser configuration parameters to parse CSV file using TD file
     * input plugin
     *
     * @param schemaConfig - Immutable Builder list of schema having columns and data                     types
     * @return Immutable Map of configuration parameter name and value
     */
    public static ImmutableMap<String, Object> parserConfig(final ImmutableList<Object> schemaConfig)
    {
        return new ImmutableMap.Builder<String, Object>()
                .put("type", "csv")
                .put("delimiter", ",")
                .put("quote", "\"")
                .put("escape", "\"")
                .put("trim_if_not_quoted", false)
                .put("skip_header_lines", 1)
                .put("allow_extra_columns", false)
                .put("allow_optional_columns", false)
                .put("columns", schemaConfig)
                .build();
    }

    /**
     * Setup schema from exists config & PowerBI plugin. Then do transaction.
     *
     * @param config the config
     * @param plugin the plugin
     * @return the schema
     */
    public static Schema doSetUpSchemaAndRun(final ConfigSource config, final MailchimpOutputPlugin plugin)
    {
        Schema schema = config.getNested("parser")
                .loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig()
                .toSchema();

        config.loadConfig(MailchimpOutputPluginDelegate.PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });

        return schema;
    }

    public static ConfigSource existsConfig(final String resourcesPath, final ConfigSource baseConfig)
    {
        return Exec
                .newConfigSource()
                .set("in", inputConfig(resourcesPath))
                .set("parser", parserConfig(schemaConfig()))
                .merge(baseConfig);
    }
}

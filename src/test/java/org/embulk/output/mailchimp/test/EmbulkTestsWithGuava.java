package org.embulk.output.mailchimp.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Created by thangnc on 2/17/17.
 */
public class EmbulkTestsWithGuava
{
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new JodaModule());
    private static final ModelManager model = new ModelManager(null, mapper);

    private EmbulkTestsWithGuava()
    {
    }

    private static ConfigLoader newSystemConfigLoader()
    {
        return new ConfigLoader(model);
    }

    public static ConfigSource config(String envName)
    {
        String path = System.getenv(envName);
        assumeThat(isNullOrEmpty(path), is(false));
        try {
            return newSystemConfigLoader().fromYamlFile(new File(path));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }
}

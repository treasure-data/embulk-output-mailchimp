package org.embulk.output.mailchimp;

import org.embulk.base.restclient.RestClientOutputPluginBase;
import org.embulk.util.config.ConfigMapperFactory;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailChimpOutputPlugin
        extends RestClientOutputPluginBase<MailChimpOutputPluginDelegate.PluginTask>
{
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    /**
     * Instantiates a new @{@link MailChimpOutputPlugin}.
     */
    public MailChimpOutputPlugin()
    {
        super(CONFIG_MAPPER_FACTORY, MailChimpOutputPluginDelegate.PluginTask.class, new MailChimpOutputPluginDelegate());
    }
}

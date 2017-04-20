package org.embulk.output.mailchimp;

import org.embulk.base.restclient.RestClientOutputPluginBase;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailChimpOutputPlugin
        extends RestClientOutputPluginBase<MailChimpOutputPluginDelegate.PluginTask>
{
    /**
     * Instantiates a new @{@link MailChimpOutputPlugin}.
     */
    public MailChimpOutputPlugin()
    {
        super(MailChimpOutputPluginDelegate.PluginTask.class, new MailChimpOutputPluginDelegate());
    }
}

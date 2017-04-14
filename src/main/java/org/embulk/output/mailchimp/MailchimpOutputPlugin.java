package org.embulk.output.mailchimp;

import org.embulk.base.restclient.RestClientOutputPluginBase;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailchimpOutputPlugin
        extends RestClientOutputPluginBase<MailchimpOutputPluginDelegate.PluginTask>
{
    public MailchimpOutputPlugin()
    {
        super(MailchimpOutputPluginDelegate.PluginTask.class, new MailchimpOutputPluginDelegate());
    }
}

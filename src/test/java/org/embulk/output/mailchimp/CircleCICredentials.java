package org.embulk.output.mailchimp;

import org.embulk.config.ConfigSource;

import static org.embulk.output.mailchimp.MailChimpOutputPlugin.CONFIG_MAPPER_FACTORY;

/**
 * Created by thangnc on 3/6/17.
 */
public class CircleCICredentials
{
    private CircleCICredentials()
    {
    }

    public static ConfigSource credentials()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("apikey", System.getenv("MAILCHIMP_APIKEY"))
                .set("access_token", System.getenv("MAILCHIMP_ACCESS_TOKEN"))
                .set("list_id", System.getenv("MAILCHIMP_LIST_ID"));
    }
}

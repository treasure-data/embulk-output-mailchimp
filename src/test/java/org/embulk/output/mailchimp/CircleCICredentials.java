package org.embulk.output.mailchimp;

import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;

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
        return EmbulkEmbed.newSystemConfigLoader().newConfigSource()
                .set("client_id", System.getenv("MAILCHIMP_CLIENT_ID"))
                .set("client_secret", System.getenv("MAILCHIMP_CLIENT_SECRET"))
                .set("code", System.getenv("MAILCHIMP_CODE"));
    }
}

package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

/**
 * Created by thangnc on 4/26/17.
 */
public enum MemberStatus
{
    /**
     * Subscribed member status.
     */
    SUBSCRIBED("subscribed"),
    /**
     * Pending member status. This status will enable to send confirmation email to user
     */
    PENDING("pending"),
    /**
     * Unsubscribed member status.
     */
    UNSUBSCRIBED("unsubscribed"),
    /**
     * Cleaned member status. Remove out of list of members but keep log
     */
    CLEANED("cleaned");

    private String type;

    MemberStatus(final String type)
    {
        this.type = type;
    }

    /**
     * Gets type.
     *
     * @return the type
     */
    public String getType()
    {
        return type;
    }

    /**
     * Find by type auth method.
     *
     * @param type the type
     * @return the auth method
     */
    @JsonCreator
    public static MemberStatus findByType(final String type)
    {
        for (MemberStatus method : values()) {
            if (method.getType().equals(type.toLowerCase())) {
                return method;
            }
        }

        throw new ConfigException(
                String.format("Unknown target '%s'. Supported statuses are [subscribed, pending, unsubscribed, cleaned]",
                              type));
    }
}

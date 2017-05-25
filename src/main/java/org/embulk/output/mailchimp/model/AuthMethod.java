package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

/**
 * Created by thangnc on 4/17/17.
 * <p>
 * MailChimp v3 supports 2 types of auth: OAuth and API key.
 */
public enum AuthMethod
{
    /**
     * OAuth2 type
     */
    OAUTH("oauth"),
    /**
     * API key type
     */
    API_KEY("api_key");

    private String type;

    AuthMethod(final String type)
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
    public static AuthMethod findByType(final String type)
    {
        for (AuthMethod method : values()) {
            if (method.getType().equals(type.toLowerCase())) {
                return method;
            }
        }

        throw new ConfigException(
                String.format("Unknown auth_method '%s'. Supported targets are [api_key, oauth]",
                              type));
    }
}

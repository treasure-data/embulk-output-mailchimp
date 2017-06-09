package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.config.ConfigException;

/**
 * Created by thangnc on 6/9/17.
 */
public enum AddressMergeFieldAttribute
{
    ADDR1("addr1"), ADDR2("addr2"), CITY("city"), STATE("state"), ZIP("zip"), COUNTRY("country");

    private String name;

    AddressMergeFieldAttribute(String type)
    {
        this.name = type;
    }

    /**
     * Gets name.
     *
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Find by name method.
     *
     * @param name the name
     * @return the auth method
     */
    @JsonCreator
    public static AddressMergeFieldAttribute findByName(final String name)
    {
        for (AddressMergeFieldAttribute method : values()) {
            if (method.getName().equals(name.toLowerCase())) {
                return method;
            }
        }

        throw new ConfigException(
                String.format("Unknown attributes '%s'. Supported attributes are [addr1, addr1, state, zip, country]",
                              name));
    }
}

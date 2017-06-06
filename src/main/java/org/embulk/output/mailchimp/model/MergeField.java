package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.config.ConfigException;

/**
 * Created by thangnc on 5/8/17.
 */
public class MergeField
{
    @JsonProperty("merge_id")
    private int mergeId;

    private String name;
    private String tag;
    private String type;

    public int getMergeId()
    {
        return mergeId;
    }

    public void setMergeId(int mergeId)
    {
        this.mergeId = mergeId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getTag()
    {
        return tag;
    }

    public void setTag(String tag)
    {
        this.tag = tag;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public enum MergeFieldType
    {
        TEXT("text"), NUMBER("number"), ADDRESS("address"), PHONE("phone"), DATE("date"), URL("url"),
        IMAGEURL("imageurl"), RADIO("radio"), DROPDOWN("dropdown"), BIRTHDAY("birthday"), ZIP("zip");

        private String type;

        MergeFieldType(String type)
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
        public static MergeFieldType findByType(final String type)
        {
            for (MergeFieldType method : values()) {
                if (method.getType().equals(type.toLowerCase())) {
                    return method;
                }
            }

            throw new ConfigException(
                    String.format("Unknown merge field type '%s'. Supported targets are [text, number, address, phone, " +
                                          "date, url, imageurl, radio, dropdown, birthday, zip]",
                                  type));
        }
    }
}

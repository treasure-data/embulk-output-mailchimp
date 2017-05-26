package org.embulk.output.mailchimp.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Created by thangnc on 4/26/17.
 */
public final class MailChimpHelper
{
    private MailChimpHelper()
    {
    }

    /**
     * Mask email string.
     *
     * @param email the email
     * @return the string
     */
    public static String maskEmail(final String email)
    {
        return email.replaceAll("(?<=.).(?=[^@]*?..@)", "*");
    }

    /**
     * This method help to get explicit merge fields with column schema without case-sensitive
     *
     * @param s    the s
     * @param list the list
     * @return the boolean
     */
    public static String containsCaseInsensitive(final String s, final List<String> list)
    {
        for (String string : list) {
            if (string.equalsIgnoreCase(s)) {
                return string;
            }
        }

        return "";
    }

    /**
     * Extract member status to validate.
     *
     * @param data the data
     * @return the multimap
     */
    public static Multimap<String, JsonNode> extractMemberStatus(final List<JsonNode> data)
    {
        Function<JsonNode, String> function = new Function<JsonNode, String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable JsonNode input)
            {
                return input != null ? input.findPath("status").asText() : "";
            }
        };

        return Multimaps.index(data, function);
    }
}

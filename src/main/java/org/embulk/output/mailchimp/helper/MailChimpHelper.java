package org.embulk.output.mailchimp.helper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.embulk.output.mailchimp.model.AddressMergeFieldAttribute;

import javax.annotation.Nullable;

import java.io.IOException;
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

    /**
     * From comma separated string list.
     *
     * @param string the string
     * @return the list
     */
    public static List<String> fromCommaSeparatedString(final String string)
    {
        Iterable<String> split = Splitter.on(",").omitEmptyStrings().trimResults().split(string);
        return Lists.newArrayList(split);
    }

    /**
     * TODO: td-worker automatically converts Presto json type to Embulk string type. This is wordaround to convert String to JsonNode
     *
     * @param string the string
     * @return the json node
     */
    public static JsonNode toJsonNode(final String string)
    {
        final ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        try {
            return mapper.readTree(string);
        }
        catch (IOException e) {
            return JsonNodeFactory.instance.nullNode();
        }
    }

    /**
     * MailChimp API requires MERGE field's type is address that have to json with keys in order.
     * {"addr1": "a", "addr2": "a1", "city": "c", "state": "s", "zip": "z", "country": "c"}
     *
     * @param originalNode the original node
     * @param attrsInOrder the keys in order
     * @return the object node
     */
    public static ObjectNode orderJsonNode(final JsonNode originalNode, final AddressMergeFieldAttribute[] attrsInOrder)
    {
        ObjectNode orderedNode = JsonNodeFactory.instance.objectNode();
        for (AddressMergeFieldAttribute attr : attrsInOrder) {
            orderedNode.put(attr.getName(),
                            originalNode.findValue(attr.getName()) != null ? originalNode.findValue(attr.getName()).asText() : "");
        }

        return orderedNode;
    }
}

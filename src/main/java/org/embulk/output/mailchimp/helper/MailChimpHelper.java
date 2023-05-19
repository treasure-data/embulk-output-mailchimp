package org.embulk.output.mailchimp.helper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.embulk.output.mailchimp.model.AddressMergeFieldAttribute;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

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

    public static Optional<JsonNode> jsonGetIgnoreCase(JsonNode node, String fieldName)
    {
        requireNonNull(fieldName);
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String curFieldName = fieldNames.next();
            if (fieldName.equalsIgnoreCase((curFieldName))) {
                return Optional.of(node.get(curFieldName));
            }
        }
        return Optional.empty();
    }

    public static Set<String> caseInsensitiveColumnNames(Schema schema)
    {
        Set<String> columns = new TreeSet<>(CASE_INSENSITIVE_ORDER);
        columns.addAll(
                schema.getColumns()
                        .stream()
                        .map(Column::getName)
                        .collect(toSet()));
        return columns;
    }
}

package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.List;

/**
 * Created by thangnc on 4/25/17.
 */
public class MailChimpRecordBuffer extends MailChimpAbstractRecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpRecordBuffer.class);
    private static final String MAILCHIMP_API = "https://us15.api.mailchimp.com";
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private MailChimpHttpClient client;

    public MailChimpRecordBuffer(Schema schema, MailChimpOutputPluginDelegate.PluginTask task)
    {
        super(schema, task);
        client = new MailChimpHttpClient(task);
    }

    @Override
    void cleanUp()
    {
        client.close();
    }

    /**
     * Build an array of email subscribers and batch insert via bulk MailChimp API
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#create-post_lists_list_id
     *
     * @param contactsData the contacts data
     * @param task         the task
     * @throws JsonProcessingException the json processing exception
     */
    @Override
    public void push(final List<JsonNode> contactsData, MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        LOG.info("Start to process subscribe data");
        String endpoint = MessageFormat.format(MAILCHIMP_API + "/3.0/lists/{0}",
                                               task.getListId());

        ArrayNode arrayOfEmailSubscribers = jsonMapper.createArrayNode();

        for (JsonNode contactData : contactsData) {
            ObjectNode property = jsonMapper.createObjectNode();
            property.put("email_address", contactData.findPath("email").asText());
            property.put("status", contactData.findPath("status").asText());

            ObjectNode mergeFields = jsonMapper.createObjectNode();
            for (final Column column : getSchema().getColumns()) {
                if (task.getMergeFields().isPresent() && task.getMergeFields().get().contains(column.getName())) {
                    String value = contactData.findValue(column.getName()).asText();
                    mergeFields.put(column.getName(), value);
                }
            }
            property.set("merged_fields", mergeFields);
            arrayOfEmailSubscribers.add(property);
        }

        ObjectNode subscribers = jsonMapper.createObjectNode();
        subscribers.putArray("members").addAll(arrayOfEmailSubscribers);
        subscribers.put("update_existing", task.getUpdateExisting());

        String content = jsonMapper.writeValueAsString(subscribers);
        JsonNode response = client.sendRequest(endpoint, HttpMethod.POST, content, task);
        List<String> errors = response.findPath("errors").findValuesAsText("error");

        StringBuilder errorMessage = new StringBuilder();
        if (!errors.isEmpty()) {
            for (String error : errors) {
                errorMessage.append("\n").append(error);
            }

            Throwables.propagate(new DataException(errorMessage.toString()));
        }
    }
}

package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.output.mailchimp.helper.MailChimpHelper;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.List;

import static org.embulk.output.mailchimp.helper.MailChimpHelper.containsCaseInsensitive;

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
    public ReportResponse push(final List<JsonNode> contactsData, MailChimpOutputPluginDelegate.PluginTask task)
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
            // The reason to use this kind of loop because we need to get explicit merge field instead of column name
            if (task.getMergeFields().isPresent() && !task.getMergeFields().get().isEmpty()) {
                for (int i = 0; i < getSchema().getColumns().size(); i++) {
                    String columnName = getSchema().getColumnName(i);
                    String mergeField = containsCaseInsensitive(columnName,
                                                                task.getMergeFields().get());

                    if (!mergeField.isEmpty()) {
                        String value = contactData.findValue(columnName).asText();
                        mergeFields.put(mergeField, value);
                    }
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
        return jsonMapper.treeToValue(response, ReportResponse.class);
    }

    @Override
    void handleErrors(List<ErrorResponse> errorResponses)
    {
        if (!errorResponses.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();

            for (ErrorResponse errorResponse : errorResponses) {
                errorMessage.append(MessageFormat.format("\nEmail `{0}` failed cause `{1}`",
                                                         MailChimpHelper.maskEmail(errorResponse.getEmailAddress()),
                                                         MailChimpHelper.maskEmail(errorResponse.getError())));
            }

            LOG.error(errorMessage.toString());
        }
    }
}

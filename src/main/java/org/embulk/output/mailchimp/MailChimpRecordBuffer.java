package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
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

/**
 * Created by thangnc on 4/25/17.
 */
public class MailChimpRecordBuffer extends MailChimpAbstractRecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpRecordBuffer.class);
    private static final String MAILCHIMP_API = "https://us15.api.mailchimp.com";
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
     * @param node the data
     * @param task the task
     * @throws JsonProcessingException the json processing exception
     */
    @Override
    public ReportResponse push(final ObjectNode node, MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        LOG.info("Start to process subscribe data");
        String endpoint = MessageFormat.format(MAILCHIMP_API + "/3.0/lists/{0}",
                                               task.getListId());

        String payload = getMapper().writeValueAsString(node);
        JsonNode response = client.sendRequest(endpoint, HttpMethod.POST, payload, task);
        return getMapper().treeToValue(response, ReportResponse.class);
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

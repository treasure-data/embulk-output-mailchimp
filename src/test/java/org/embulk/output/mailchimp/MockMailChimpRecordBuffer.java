package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.Schema;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Created by thangnc on 5/8/17.
 */
public class MockMailChimpRecordBuffer extends MailChimpAbstractRecordBuffer
{
    private MockMailChimpRecordBuffer(final Schema schema, final MailChimpOutputPluginDelegate.PluginTask pluginTask)
    {
        super(schema, pluginTask);
    }

    @Override
    void cleanUp()
    {

    }

    @Override
    ReportResponse push(ObjectNode node, MailChimpOutputPluginDelegate.PluginTask task) throws JsonProcessingException
    {
        return null;
    }

    @Override
    void handleErrors(List<ErrorResponse> errorResponses)
    {

    }

    @Override
    Map<String, String> findIdsByCategoryName(MailChimpOutputPluginDelegate.PluginTask task) throws JsonProcessingException
    {
        return null;
    }

    public static MockMailChimpRecordBuffer createAndSetupMocks(final Schema schema, final MailChimpOutputPluginDelegate.PluginTask task)
            throws Exception
    {
        MockMailChimpRecordBuffer mailChimpRecordBuffer = spy(new MockMailChimpRecordBuffer(schema, task));
        when(mailChimpRecordBuffer.findIdsByCategoryName(task)).thenReturn(new HashMap<String, String>());
        when(mailChimpRecordBuffer.push(JsonNodeFactory.instance.objectNode(), task)).thenReturn(new ReportResponse());
        return mailChimpRecordBuffer;
    }
}

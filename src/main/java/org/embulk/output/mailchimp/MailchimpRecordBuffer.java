package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailchimpRecordBuffer
        extends RecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailchimpRecordBuffer.class);
    private static final int MAX_RECORD_PER_BATCH_REQUEST = 500;
    private final String attributeName;
    private final MailchimpOutputPluginDelegate.PluginTask task;
    private final MailchimpHttpClient client;
    private final ObjectMapper mapper;
    private int requestCount;
    private long totalCount;
    private List<JsonNode> records;

    public MailchimpRecordBuffer(final String attributeName, final MailchimpOutputPluginDelegate.PluginTask task)
    {
        this.attributeName = attributeName;
        this.task = task;
        this.client = new MailchimpHttpClient(task);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = new ArrayList<>();
    }

    @Override
    public void bufferRecord(ServiceRecord serviceRecord)
    {
        JacksonServiceRecord jacksonServiceRecord;

        try {
            jacksonServiceRecord = (JacksonServiceRecord) serviceRecord;
            JsonNode record = mapper.readTree(jacksonServiceRecord.toString()).get("record");

            requestCount++;
            totalCount++;

            records.add(record);
            if (requestCount >= MAX_RECORD_PER_BATCH_REQUEST) {
                pushData(records);

                if (totalCount % 1000 == 0) {
                    LOG.info("Inserted {} records", totalCount);
                }

                records = new ArrayList<>();
                requestCount = 0;
            }
        } catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        if (records.size() > 0) {
            pushData(records);
            LOG.info("Inserted {} records", records.size());
        }

        this.client.close();
        return Exec.newTaskReport().set("inserted", totalCount);
    }

    private void pushData(final List<JsonNode> data)
    {
        try {
            client.push(data, task);
        } catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
    }
}

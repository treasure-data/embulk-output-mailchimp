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
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thangnc on 4/14/17.
 */
public abstract class MailChimpAbstractRecordBuffer
        extends RecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpAbstractRecordBuffer.class);
    private static final int MAX_RECORD_PER_BATCH_REQUEST = 500;
    private final MailChimpOutputPluginDelegate.PluginTask task;
    private final ObjectMapper mapper;
    private final Schema schema;
    private int requestCount;
    private long totalCount;
    private List<JsonNode> records;

    public MailChimpAbstractRecordBuffer(final Schema schema, final MailChimpOutputPluginDelegate.PluginTask task)
    {
        this.schema = schema;
        this.task = task;
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
                push(records, task);

                if (totalCount % 1000 == 0) {
                    LOG.info("Inserted {} records", totalCount);
                }

                records = new ArrayList<>();
                requestCount = 0;
            }
        }
        catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
        catch (ClassCastException ex) {
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public TaskReport commitWithTaskReportUpdated(TaskReport taskReport)
    {
        try {
            if (records.size() > 0) {
                push(records, task);
                LOG.info("Inserted {} records", records.size());
            }

            cleanUp();
            return Exec.newTaskReport().set("inserted", totalCount);
        }
        catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
    }

    public Schema getSchema()
    {
        return schema;
    }

    abstract void cleanUp();

    abstract void push(final List<JsonNode> data, final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException;
}

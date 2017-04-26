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
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
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

    /**
     * Instantiates a new Mail chimp abstract record buffer.
     *
     * @param schema the schema
     * @param task   the task
     */
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
                ReportResponse reportResponse = push(records, task);

                if (totalCount % 1000 == 0) {
                    LOG.info("Pushed {} records", totalCount);
                }

                LOG.info("{} records created, {} records updated, {} records failed",
                         reportResponse.getTotalCreated(),
                         reportResponse.getTotalUpdated(),
                         reportResponse.getErrorCount());
                handleErrors(reportResponse.getErrors());

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
                ReportResponse reportResponse = push(records, task);
                LOG.info("Pushed {} records", records.size());
                LOG.info("{} records created, {} records updated, {} records failed",
                         reportResponse.getTotalCreated(),
                         reportResponse.getTotalUpdated(),
                         reportResponse.getErrorCount());
                handleErrors(reportResponse.getErrors());
            }

            cleanUp();
            return Exec.newTaskReport().set("pushed", totalCount);
        }
        catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
    }

    /**
     * Gets schema.
     *
     * @return the schema
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Clean up.
     */
    abstract void cleanUp();

    /**
     * Push payload data to MailChimp API and get @{@link ReportResponse}
     *
     * @param data the data
     * @param task the task
     * @return the report response
     * @throws JsonProcessingException the json processing exception
     */
    abstract ReportResponse push(final List<JsonNode> data, final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException;

    /**
     * Handle @{@link ErrorResponse} from MailChimp API if exists.
     *
     * @param errorResponses the error responses
     */
    abstract void handleErrors(final List<ErrorResponse> errorResponses);
}

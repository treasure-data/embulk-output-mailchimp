package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by thangnc on 4/14/17.
 */
public abstract class MailChimpAbstractRecordBuffer
        extends RecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpAbstractRecordBuffer.class);
    protected static final String MAILCHIMP_API = "https://us15.api.mailchimp.com";
    private static final int MAX_RECORD_PER_BATCH_REQUEST = 500;
    private final MailChimpOutputPluginDelegate.PluginTask task;
    private final ObjectMapper mapper;
    private final Schema schema;
    private int requestCount;
    private long totalCount;
    private List<JsonNode> records;
    private Map<String, String> categories;

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
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = new ArrayList<>();
        this.categories = new HashMap<>();

        // Should loop the ids and get the name of interest categories.
        // The reason why we put categories validation here because we can not share data between instance.
        try {
            categories = findIdsByCategoryName(task);
            if (task.getInterestCategories().isPresent()
                    && categories.size() != task.getInterestCategories().get().size()) {
                throw new ConfigException("Invalid interest category names");
            }
        }
        catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
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
                ObjectNode subcribers = processSubcribers(records, task);
                ReportResponse reportResponse = push(subcribers, task);

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
                ObjectNode subcribers = processSubcribers(records, task);
                ReportResponse reportResponse = push(subcribers, task);
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
     * Receive data and build payload json that contains subscribers
     *
     * @param data the data
     * @param task the task
     * @return the object node
     */
    ObjectNode processSubcribers(final List<JsonNode> data, final MailChimpOutputPluginDelegate.PluginTask task)
    {
        LOG.info("Start to process subscribe data");

        ArrayNode arrayOfEmailSubscribers = mapper.createArrayNode();

        for (JsonNode contactData : data) {
            ObjectNode property = JsonNodeFactory.instance.objectNode();
            property.put("email_address", contactData.findPath("email").asText());
            property.put("status", contactData.findPath("status").asText());

            ObjectNode mergeFields = JsonNodeFactory.instance.objectNode();
            if (task.getMergeFields().isPresent() && !task.getMergeFields().get().isEmpty()) {
                for (final Column column : schema.getColumns()) {
                    if (task.getMergeFields().get().contains(column.getName().toUpperCase())) {
                        String value = contactData.findValue(column.getName()).asText();
                        mergeFields.put(column.getName().toUpperCase(), value);
                    }
                }
            }

            ObjectNode interests = JsonNodeFactory.instance.objectNode();
            if (task.getInterestCategories().isPresent() && !task.getInterestCategories().get().isEmpty()
                    && !categories.keySet().isEmpty()) {
                for (final Column column : schema.getColumns()) {
                    if (categories.keySet().contains(column.getName().toLowerCase())) {
                        String value = contactData.findValue(column.getName()).asText();
                        interests.put(categories.get(column.getName().toLowerCase()), Boolean.valueOf(value).booleanValue());
                    }
                }
            }

            property.set("merged_fields", mergeFields);
            property.set("interests", interests);
            arrayOfEmailSubscribers.add(property);
        }

        ObjectNode subscribers = JsonNodeFactory.instance.objectNode();
        subscribers.putArray("members").addAll(arrayOfEmailSubscribers);
        subscribers.put("update_existing", task.getUpdateExisting());
        return subscribers;
    }

    /**
     * Gets mapper.
     *
     * @return the mapper
     */
    public ObjectMapper getMapper()
    {
        return mapper;
    }

    /**
     * Clean up.
     */
    abstract void cleanUp();

    /**
     * Push payload data to MailChimp API and get @{@link ReportResponse}
     *
     * @param node the content
     * @param task the task
     * @return the report response
     * @throws JsonProcessingException the json processing exception
     */
    abstract ReportResponse push(final ObjectNode node, final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException;

    /**
     * Handle @{@link ErrorResponse} from MailChimp API if exists.
     *
     * @param errorResponses the error responses
     */
    abstract void handleErrors(final List<ErrorResponse> errorResponses);

    abstract Map<String, String> findIdsByCategoryName(final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException;
}

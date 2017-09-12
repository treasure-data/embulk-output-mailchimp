package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordBuffer;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.TaskReport;
import org.embulk.output.mailchimp.model.AddressMergeFieldAttribute;
import org.embulk.output.mailchimp.model.InterestResponse;
import org.embulk.output.mailchimp.model.MergeField;
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

import static org.embulk.output.mailchimp.MailChimpOutputPluginDelegate.PluginTask;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.containsCaseInsensitive;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.fromCommaSeparatedString;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.orderJsonNode;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.toJsonNode;
import static org.embulk.output.mailchimp.model.MemberStatus.PENDING;
import static org.embulk.output.mailchimp.model.MemberStatus.SUBSCRIBED;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailChimpRecordBuffer
        extends RecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpRecordBuffer.class);
    private static final int MAX_RECORD_PER_BATCH_REQUEST = 500;
    private final MailChimpOutputPluginDelegate.PluginTask task;
    private final MailChimpClient mailChimpClient;
    private final ObjectMapper mapper;
    private final Schema schema;
    private int requestCount;
    private long totalCount;
    private List<JsonNode> records;
    private Map<String, Map<String, InterestResponse>> categories;
    private Map<String, MergeField> availableMergeFields;

    /**
     * Instantiates a new Mail chimp abstract record buffer.
     *
     * @param schema the schema
     * @param task   the task
     */
    public MailChimpRecordBuffer(final Schema schema, final PluginTask task)
    {
        this.schema = schema;
        this.task = task;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        this.records = new ArrayList<>();
        this.categories = new HashMap<>();
        this.mailChimpClient = new MailChimpClient(task);
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
                ReportResponse reportResponse = mailChimpClient.push(subcribers, task);

                if (totalCount % 1000 == 0) {
                    LOG.info("Pushed {} records", totalCount);
                }

                LOG.info("Response from MailChimp: {} records created, {} records updated, {} records failed",
                         reportResponse.getTotalCreated(),
                         reportResponse.getTotalUpdated(),
                         reportResponse.getErrorCount());
                mailChimpClient.handleErrors(reportResponse.getErrors());

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
                ReportResponse reportResponse = mailChimpClient.push(subcribers, task);
                LOG.info("Pushed {} records", records.size());
                LOG.info("Response from MailChimp: {} records created, {} records updated, {} records failed",
                         reportResponse.getTotalCreated(),
                         reportResponse.getTotalUpdated(),
                         reportResponse.getErrorCount());
                mailChimpClient.handleErrors(reportResponse.getErrors());
            }

            return Exec.newTaskReport().set("pushed", totalCount);
        }
        catch (JsonProcessingException jpe) {
            throw new DataException(jpe);
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public void finish()
    {
        // Do not close here
    }

    @Override
    public void close()
    {
        // Do not implement
    }

    /**
     * Receive data and build payload json that contains subscribers
     *
     * @param data the data
     * @param task the task
     * @return the object node
     */
    private ObjectNode processSubcribers(final List<JsonNode> data, final PluginTask task)
            throws JsonProcessingException
    {
        LOG.info("Start to process subscriber data");

        // Should loop the names and get the id of interest categories.
        // The reason why we put categories validation here because we can not share data between instance.
        categories = mailChimpClient.extractInterestCategoriesByGroupNames(task);

        // Extract merge fields detail
        availableMergeFields = mailChimpClient.extractMergeFieldsFromList(task);

        // Required merge fields
        Map<String, String> map = new HashMap<>();
        map.put("FNAME", task.getFnameColumn());
        map.put("LNAME", task.getLnameColumn());

        List<JsonNode> subscribersList = FluentIterable.from(data)
                .transform(contactMapper(map))
                .toList();

        ObjectNode subscribers = JsonNodeFactory.instance.objectNode();
        subscribers.putArray("members").addAll(subscribersList);
        subscribers.put("update_existing", task.getUpdateExisting());
        return subscribers;
    }

    private Function<JsonNode, JsonNode> contactMapper(final Map<String, String> allowColumns)
    {
        return new Function<JsonNode, JsonNode>()
        {
            @Override
            public JsonNode apply(JsonNode input)
            {
                ObjectNode property = JsonNodeFactory.instance.objectNode();
                property.put("email_address", input.findPath(task.getEmailColumn()).asText());
                property.put("status", task.getDoubleOptIn() ? PENDING.getType() : SUBSCRIBED.getType());
                ObjectNode mergeFields = JsonNodeFactory.instance.objectNode();
                for (String allowColumn : allowColumns.keySet()) {
                    String value = input.hasNonNull(allowColumns.get(allowColumn)) ? input.findValue(allowColumns.get(allowColumn)).asText() : "";
                    mergeFields.put(allowColumn, value);
                }

                // Update additional merge fields if exist
                if (task.getMergeFields().isPresent() && !task.getMergeFields().get().isEmpty()) {
                    for (final Column column : schema.getColumns()) {
                        if (!"".equals(containsCaseInsensitive(column.getName(), task.getMergeFields().get()))) {
                            String value = input.hasNonNull(column.getName()) ? input.findValue(column.getName()).asText() : "";

                            // Try to convert to Json from string with the merge field's type is address
                            if (availableMergeFields.get(column.getName()).getType()
                                    .equals(MergeField.MergeFieldType.ADDRESS.getType())) {
                                JsonNode addressNode = toJsonNode(value);
                                if (addressNode instanceof NullNode) {
                                    mergeFields.put(column.getName().toUpperCase(), value);
                                }
                                else {
                                    mergeFields.set(column.getName().toUpperCase(),
                                                    orderJsonNode(addressNode, AddressMergeFieldAttribute.values()));
                                }
                            }
                            else {
                                mergeFields.put(column.getName().toUpperCase(), value);
                            }
                        }
                    }
                }

                property.set("merge_fields", mergeFields);

                // Update interest categories if exist
                if (task.getGroupingColumns().isPresent() && !task.getGroupingColumns().get().isEmpty()) {
                    property.set("interests", buildInterestCategories(task, input));
                }

                // Update language if exist
                if (task.getLanguageColumn().isPresent() && !task.getLanguageColumn().get().isEmpty()) {
                    property.put("language", input.findPath(task.getLanguageColumn().get()).asText());
                }

                return property;
            }
        };
    }

    private ObjectNode buildInterestCategories(final MailChimpOutputPluginDelegate.PluginTask task,
                                               final JsonNode input)
    {
        ObjectNode interests = JsonNodeFactory.instance.objectNode();

        for (String category : task.getGroupingColumns().get()) {
            String inputValue = input.findValue(category).asText();
            List<String> interestValues = fromCommaSeparatedString(inputValue);
            Map<String, InterestResponse> availableCategories = categories.get(category);

            // Only update user-predefined categories if replace interests != true
            if (!task.getReplaceInterests()) {
                for (String interestValue : interestValues) {
                    if (availableCategories.get(interestValue) != null) {
                        interests.put(availableCategories.get(interestValue).getId(), true);
                    }
                }
            } // Otherwise, force update all categories include user-predefined categories
            else if (task.getReplaceInterests()) {
                for (String availableCategory : availableCategories.keySet()) {
                    if (interestValues.contains(availableCategory)) {
                        interests.put(availableCategories.get(availableCategory).getId(), true);
                    }
                    else {
                        interests.put(availableCategories.get(availableCategory).getId(), false);
                    }
                }
            }
        }

        return interests;
    }
}

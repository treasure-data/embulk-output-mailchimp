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
import com.google.common.base.Optional;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static org.embulk.output.mailchimp.MailChimpOutputPluginDelegate.PluginTask;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.fromCommaSeparatedString;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.jsonGetIgnoreCase;
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
    private final MailChimpOutputPluginDelegate.PluginTask task;
    private final MailChimpClient mailChimpClient;
    private final ObjectMapper mapper;
    private final Schema schema;
    private int requestCount;
    private int errorCount;
    private long totalCount;
    private List<JsonNode> records;
    private Map<String, Map<String, InterestResponse>> categories;
    private Map<String, MergeField> availableMergeFields;
    private List<JsonNode> uniqueRecords;
    private List<JsonNode> duplicatedRecords;

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
        this.uniqueRecords = new ArrayList<>();
        this.duplicatedRecords = new ArrayList<>();
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
            if (requestCount >= task.getMaxRecordsPerRequest()) {
                filterDuplicatedRecords();
                pushData();

                if (totalCount % 1000 == 0) {
                    LOG.info("Pushed {} records", totalCount);
                }

                records = new ArrayList<>();
                uniqueRecords = new ArrayList<>();
                duplicatedRecords = new ArrayList<>();
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
                filterDuplicatedRecords();
                pushData();
                records = new ArrayList<>();
                uniqueRecords = new ArrayList<>();
                duplicatedRecords = new ArrayList<>();
            }
            return Exec.newTaskReport().set("pushed", totalCount).set("error_count", errorCount);
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
//      // Do not close here
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
        // Should loop the names and get the id of interest categories.
        // The reason why we put categories validation here because we can not share data between instance.
        if (categories == null) {
            categories = mailChimpClient.extractInterestCategoriesByGroupNames(task, schema);
        }

        // Extract merge fields detail
        if (availableMergeFields == null) {
            availableMergeFields = mailChimpClient.extractMergeFieldsFromList(task);
        }

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
                    Map<String, String> columnNameCaseInsensitiveLookup = new TreeMap<>(CASE_INSENSITIVE_ORDER);
                    for (Column col : schema.getColumns()) {
                        columnNameCaseInsensitiveLookup.put(col.getName(), col.getName());
                    }
                    Map<String, MergeField> availableMergeFieldsCaseInsensitiveLookup = new TreeMap<>(CASE_INSENSITIVE_ORDER);
                    availableMergeFieldsCaseInsensitiveLookup.putAll(availableMergeFields);

                    for (String field : task.getMergeFields().get()) {
                        if (!columnNameCaseInsensitiveLookup.containsKey(field)) {
                            LOG.warn(format("Field '%s' is configured on data transfer but cannot be found on any columns.", field));
                            continue;
                        }
                        String columnName = columnNameCaseInsensitiveLookup.get(field);
                        if (!availableMergeFieldsCaseInsensitiveLookup.containsKey(columnName)) {
                            LOG.warn(format("Field '%s' is configured on data transfer but is not predefined on Mailchimp.", field));
                            continue;
                        }

                        String value = input.hasNonNull(columnName) ? input.findValue(columnName).asText() : "";

                        // Try to convert to Json from string with the merge field's type is address
                        if (availableMergeFieldsCaseInsensitiveLookup.get(columnName).getType().equals(MergeField.MergeFieldType.ADDRESS.getType())) {
                            JsonNode addressNode = toJsonNode(value);
                            if (addressNode instanceof NullNode) {
                                mergeFields.put(columnName.toUpperCase(), value);
                            }
                            else {
                                mergeFields.set(columnName.toUpperCase(),
                                        orderJsonNode(addressNode, AddressMergeFieldAttribute.values()));
                            }
                        }
                        else {
                            mergeFields.put(columnName.toUpperCase(), value);
                        }
                    }
                }

                property.set("merge_fields", mergeFields);

                // Update interest categories if exist
                if (task.getGroupingColumns().isPresent() && !task.getGroupingColumns().get().isEmpty()) {
                    property.set("interests", buildInterestCategories(input));
                }

                // Update language if exist
                if (task.getLanguageColumn().isPresent() && !task.getLanguageColumn().get().isEmpty()) {
                    property.put("language", input.findPath(task.getLanguageColumn().get()).asText());
                }

                return property;
            }
        };
    }

    private ObjectNode buildInterestCategories(final JsonNode input)
    {
        ObjectNode interests = JsonNodeFactory.instance.objectNode();

        if (task.getGroupingColumns().isPresent()) {
            for (String category : task.getGroupingColumns().get()) {
                Optional<JsonNode> inputValue = jsonGetIgnoreCase(input, category);
                if (!inputValue.isPresent()) {
                    // Silently ignore if the grouping column is absent
                    continue;
                }
                List<String> recordInterests = fromCommaSeparatedString(inputValue.get().asText());
                // `categories` is guaranteed to contain the `category` as it already did an early check
                Map<String, InterestResponse> availableInterests = categories.get(category);

                // Only update user-predefined categories if replace interests != true
                if (!task.getReplaceInterests()) {
                    for (String recordInterest : recordInterests) {
                        if (availableInterests.get(recordInterest) != null) {
                            interests.put(availableInterests.get(recordInterest).getId(), true);
                        }
                    }
                } // Otherwise, force update all categories include user-predefined categories
                else if (task.getReplaceInterests()) {
                    for (String availableInterest : availableInterests.keySet()) {
                        if (recordInterests.contains(availableInterest)) {
                            interests.put(availableInterests.get(availableInterest).getId(), true);
                        }
                        else {
                            interests.put(availableInterests.get(availableInterest).getId(), false);
                        }
                    }
                }
            }
        }

        return interests;
    }

    private void filterDuplicatedRecords()
    {
        Set<String> uniqueEmails = new HashSet<>();
        for (JsonNode node : records) {
            if (uniqueEmails.contains(node.findPath(task.getEmailColumn()).asText())) {
                duplicatedRecords.add(node);
            }
            else {
                uniqueEmails.add(node.findPath(task.getEmailColumn()).asText());
                uniqueRecords.add(node);
            }
        }
    }

    private void pushData() throws JsonProcessingException
    {
        long startTime = System.currentTimeMillis();
        ObjectNode subscribers =
                processSubcribers(uniqueRecords, task);
        ReportResponse reportResponse = mailChimpClient.push(subscribers, task);

        LOG.info("Done with {} record(s). Response from MailChimp: {} records created, {} records updated, {} records failed. Batch took {} ms ",
                 records.size(), reportResponse.getTotalCreated(),
                 reportResponse.getTotalUpdated(),
                 reportResponse.getErrorCount(), System.currentTimeMillis() - startTime);
        errorCount += reportResponse.getErrors().size();
        mailChimpClient.handleErrors(reportResponse.getErrors());

        mailChimpClient.avoidFloodAPI("Process next request", task.getSleepBetweenRequestsMillis());

        if (duplicatedRecords.size() > 0) {
            LOG.info("Start to process {} duplicated record(s)", duplicatedRecords.size());
            for (JsonNode duplicatedRecord : duplicatedRecords) {
                startTime = System.currentTimeMillis();
                subscribers = processSubcribers(Arrays.asList(duplicatedRecord), task);
                reportResponse = mailChimpClient.push(subscribers, task);

                LOG.info("Done. Response from MailChimp: {} records created, {} records updated, {} records failed. Batch took {} ms ",
                         reportResponse.getTotalCreated(),
                         reportResponse.getTotalUpdated(),
                         reportResponse.getErrorCount(), System.currentTimeMillis() - startTime);
                mailChimpClient.handleErrors(reportResponse.getErrors());
            }
        }
    }
}

package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.config.ConfigException;
import org.embulk.output.mailchimp.MailChimpOutputPluginDelegate.PluginTask;
import org.embulk.output.mailchimp.helper.MailChimpHelper;
import org.embulk.output.mailchimp.helper.MailChimpRetryable;
import org.embulk.output.mailchimp.model.Category;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.Interest;
import org.embulk.output.mailchimp.model.MergeField;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.text.MessageFormat.format;
import static java.util.Arrays.asList;

/**
 * Created by thangnc on 4/25/17.
 */
public class MailChimpClient
{
    private static final Logger LOG = Exec.getLogger(MailChimpClient.class);
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
    private StringJsonParser jsonParser = new StringJsonParser();

    /**
     * Instantiates a new Mail chimp client.
     *
     * @param task the task
     */
    public MailChimpClient(final PluginTask task)
    {
        findList(task);
    }

    /**
     * Build an array of email subscribers and batch insert via bulk MailChimp API
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#create-post_lists_list_id
     *
     * @param node the data
     * @param task the task
     * @return the report response
     */
    public ReportResponse push(final ObjectNode node, PluginTask task) throws JsonProcessingException
    {
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            String response = retryable.post(format("/lists/{0}", task.getListId()),
                                             "application/json;utf-8",
                                             node.toString());
            if (response != null && !response.isEmpty()) {
                return mapper.treeToValue(jsonParser.parseJsonObject(response), ReportResponse.class);
            }

            throw new DataException("The json data in response were broken.");
        }
    }

    /**
     * Handle detail errors after call bulk MailChimp API
     *
     * @param errorResponses the error responses
     */
    public void handleErrors(List<ErrorResponse> errorResponses)
    {
        if (!errorResponses.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();

            for (ErrorResponse errorResponse : errorResponses) {
                errorMessage.append(format("`{0}` failed cause `{1}`\n",
                                                         MailChimpHelper.maskEmail(errorResponse.getEmailAddress()),
                                                         MailChimpHelper.maskEmail(errorResponse.getError())));
            }

            LOG.error("Error response from MailChimp: ");
            LOG.error(errorMessage.toString());
        }
    }

    /**
     * Extract interest categories by group names. Loop via categories and fetch category details
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/interest-categories/#read-get_lists_list_id_interest_categories
     * https://developer.mailchimp.com/documentation/mailchimp/reference/lists/interest-categories/#read-get_lists_list_id_interest_categories_interest_category_id
     */
    Map<String, Map<String, Interest>> interestsByCategory(final PluginTask task, Schema schema) throws JsonProcessingException
    {
        if (!task.getGroupingColumns().isPresent() || task.getGroupingColumns().get().isEmpty()) {
            return Collections.emptyMap();
        }
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            List<Category> categories = fetchCategories(retryable, task.getListId(), task.getGroupingColumns().get());
            Map<String, Map<String, Interest>> interestsByCategory = new HashMap<>();
            for (Category category : categories) {
                // Skip fetching interests if this category isn't specified in the task's grouping column.
                // Assume task's grouping columns are always in lower case
                if (!task.getGroupingColumns().get().contains(category.getTitle().toLowerCase())) {
                    continue;
                }
                avoidFloodAPI("Fetching next category's interests", task.getSleepBetweenRequestsMillis());
                interestsByCategory.put(
                        category.getTitle().toLowerCase(),
                        convertInterestCategoryToMap(fetchInterests(retryable, task.getListId(), category.getId())));
            }
            return interestsByCategory;
        }
    }

    /**
     * @throws ConfigException if task having unexist category
     */
    private List<Category> fetchCategories(MailChimpRetryable retryable,
                                           String listId,
                                           List<String> taskCategories)
            throws JsonProcessingException
    {
        List<Category> categories = fetch(
                retryable,
                "/lists/" + listId + "/interest-categories",
                "categories",
                Category[].class);
        // Fail early if one of the task's categories is not exist
        taskCategories:
        for (String taskCategoryName : taskCategories) {
            for (Category category : categories) {
                // Tasks's configured categories are (implicitly?) case-sensitive
                if (category.getTitle().toLowerCase().equals(taskCategoryName)) {
                    continue taskCategories;
                }
            }
            throw new ConfigException("Invalid group category name: '" + taskCategoryName + "'");
        }
        return categories;
    }

    private List<Interest> fetchInterests(MailChimpRetryable retryable,
                                          String listId,
                                          String categoryId)
            throws JsonProcessingException
    {
        return fetch(retryable,
                "/lists/" + listId + "/interest-categories/" + categoryId + "/interests",
                "interests",
                Interest[].class);
    }
    /**
     * Extract merge fields from the list, find correct merge fields from API and put into the map to use
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/merge-fields/#read-get_lists_list_id_merge_fields
     */
    Map<String, MergeField> mergeFieldByTag(PluginTask task) throws JsonProcessingException
    {
        return convertMergeFieldToMap(
                fetch(task,
                        "/lists/" + task.getListId() + "/merge-fields",
                        "merge_fields",
                        MergeField[].class));
    }

    /**
     * Like {@link MailChimpClient#fetch(MailChimpRetryable, String, String, Class)},
     * with a automatically created MailchimpRetryable
     */
    private <T> List<T> fetch(PluginTask task,
                              String url,
                              String recordsAttribute,
                              Class<T[]> entitiesClass)
            throws JsonProcessingException
    {
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            return fetch(retryable, url, recordsAttribute, entitiesClass);
        }
    }
    /**
     * Fetch all (by pagination) records at the target URL,
     * Assume that endpoint handles `count` and `offset` parameter and have a response scheme of:
     *     {
     *        recordsAttribute}: [...],
     *        "total_items": 10
     *     }
     * @param recordsAttribute name of the attribute to extract records inside the response's body.
     * @param entitiesClass *Array* class of the entity to deserialize into
     */
    private <T> List<T> fetch(MailChimpRetryable retryable,
                              String url,
                              String recordsAttribute,
                              Class<T[]> entitiesClass)
            throws JsonProcessingException
    {
        final int batchSize = 100;
        int offset = 0;
        List<T> entities = new ArrayList<>();
        ObjectNode response;
        do {
            response = jsonParser.parseJsonObject(
                    retryable.get(url + "?count=" + batchSize + "&offset=" + offset));
            entities.addAll(asList(mapper.treeToValue(response.get(recordsAttribute), entitiesClass)));
            offset += batchSize;
        }
        while (offset < response.get("total_items").asInt());
        return entities;
    }

    private void findList(final PluginTask task)
    {
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            jsonParser.parseJsonObject(retryable.get(format("/lists/{0}",
                                                                          task.getListId())));
        }
        catch (HttpResponseException hre) {
            throw new ConfigException("The `list id` could not be found.");
        }
    }

    private Map<String, Interest> convertInterestCategoryToMap(final List<Interest> interestList)
    {
        Function<Interest, String> function = new Function<Interest, String>()
        {
            @Override
            public String apply(@Nullable Interest input)
            {
                return input.getName();
            }
        };

        return Maps.uniqueIndex(FluentIterable.from(interestList)
                                        .toList(),
                                function);
    }

    private Map<String, MergeField> convertMergeFieldToMap(final List<MergeField> mergeFieldList)
    {
        Function<MergeField, String> function = new Function<MergeField, String>()
        {
            @Nullable
            @Override
            public String apply(@Nullable MergeField input)
            {
                return input.getTag().toLowerCase();
            }
        };

        return Maps.uniqueIndex(FluentIterable.from(mergeFieldList)
                                        .toList(),
                                function);
    }

    private boolean hasMorePage(final int count, final int pageSize, final int page)
    {
        int totalPage = count / pageSize + (count % pageSize > 0 ? 1 : 0);
        return page < totalPage;
    }

    public void avoidFloodAPI(final String reason, final long millis)
    {
        try {
            LOG.info("{} in {}ms...", reason, millis);
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            LOG.warn("Failed to sleep: {}", e.getMessage());
        }
    }
}

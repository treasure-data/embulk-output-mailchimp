package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.embulk.output.mailchimp.model.CategoriesResponse;
import org.embulk.output.mailchimp.model.Category;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.Interest;
import org.embulk.output.mailchimp.model.InterestsResponse;
import org.embulk.output.mailchimp.model.MergeField;
import org.embulk.output.mailchimp.model.MergeFields;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Joiner.on;
import static java.text.MessageFormat.format;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.caseInsensitiveColumnNames;

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
     *
     * @param task the task
     * @return the map
     * @throws JsonProcessingException the json processing exception
     */
    Map<String, Map<String, Interest>> interestsByCategory(final PluginTask task, Schema schema)
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

            // Warn if schema doesn't have the task's grouping column
            Set<String> columnNames = caseInsensitiveColumnNames(schema);
            Set<String> groupNames = new HashSet<>(task.getGroupingColumns().get());
            groupNames.removeAll(columnNames);
            if (groupNames.size() > 0) {
                LOG.warn("Data schema doesn't contain the task's grouping column(s): {}", on(", ").join(groupNames));
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
    {
        final int batchSize = 100;
        int offset = 0;
        List<Category> categories = new ArrayList<>();
        CategoriesResponse categoriesResponse;
        do {
            String response = retryable.get(format(
                    "/lists/{0}/interest-categories?count={1}&offset={2}",
                    listId, batchSize, offset));
            try {
                categoriesResponse = mapper.readValue(response, CategoriesResponse.class);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            categories.addAll(categoriesResponse.getCategories());
            offset += batchSize;
        }
        while (offset < categoriesResponse.getTotalItems());

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

    // Pretty much a copy of fetchCategories,
    // "rule of 3", feel free to generalize if you see necessary
    private List<Interest> fetchInterests(MailChimpRetryable retryable,
                                          String listId,
                                          String categoryId)
    {
        final int batchSize = 100;
        int offset = 0;
        List<Interest> interests = new ArrayList<>();
        InterestsResponse interestsResponse;
        do {
            String response = retryable.get(format(
                    "/lists/{0}/interest-categories/{1}/interests?count={2}&offset={3}",
                    listId, categoryId, batchSize, offset));
            try {
                interestsResponse = mapper.readValue(response, InterestsResponse.class);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            interests.addAll(interestsResponse.getInterests());
            offset += batchSize;
        }
        while (offset < interestsResponse.getTotalItems());

        return interests;
    }

    /**
     * Extract merge fields from the list, find correct merge fields from API and put into the map to use
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/merge-fields/#read-get_lists_list_id_merge_fields
     *
     * @param task the task
     * @return the map
     * @throws JsonProcessingException the json processing exception
     */
    public Map<String, MergeField> extractMergeFieldsFromList(PluginTask task) throws JsonProcessingException
    {
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            int count = 100;
            int offset = 0;
            int page = 1;
            boolean hasMore = true;
            List<MergeField> allMergeFields = new ArrayList<>();

            while (hasMore) {
                String path = format("/lists/{0}/merge-fields?count={1}&offset={2}",
                                                   task.getListId(),
                                                   count,
                                                   offset);

                JsonNode response = jsonParser.parseJsonObject(retryable.get(path));
                MergeFields mergeFields = mapper.treeToValue(response,
                                                             MergeFields.class);

                allMergeFields.addAll(mergeFields.getMergeFields());

                if (hasMorePage(mergeFields.getTotalItems(), count, page)) {
                    offset = count;
                    page++;
                }
                else {
                    hasMore = false;
                }
            }

            return convertMergeFieldToMap(allMergeFields);
        }
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

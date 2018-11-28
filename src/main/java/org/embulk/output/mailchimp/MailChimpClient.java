package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.eclipse.jetty.client.HttpResponseException;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.config.ConfigException;
import org.embulk.output.mailchimp.MailChimpOutputPluginDelegate.PluginTask;
import org.embulk.output.mailchimp.helper.MailChimpHelper;
import org.embulk.output.mailchimp.helper.MailChimpRetryable;
import org.embulk.output.mailchimp.model.CategoriesResponse;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.InterestCategoriesResponse;
import org.embulk.output.mailchimp.model.InterestResponse;
import org.embulk.output.mailchimp.model.InterestsResponse;
import org.embulk.output.mailchimp.model.MergeField;
import org.embulk.output.mailchimp.model.MergeFields;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Joiner.on;
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
            String response = retryable.post(MessageFormat.format("/lists/{0}", task.getListId()),
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
                errorMessage.append(MessageFormat.format("`{0}` failed cause `{1}`\n",
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
    public Map<String, Map<String, InterestResponse>> extractInterestCategoriesByGroupNames(final PluginTask task,
                                                                                            Schema schema)
            throws JsonProcessingException
    {
        try (MailChimpRetryable retryable = new MailChimpRetryable(task)) {
            Map<String, Map<String, InterestResponse>> categories = new HashMap<>();
            if (task.getGroupingColumns().isPresent() && !task.getGroupingColumns().get().isEmpty()) {
                List<String> interestCategoryNames = task.getGroupingColumns().get();

                int count = 100;
                int offset = 0;
                int page = 1;
                boolean hasMore = true;
                JsonNode response;
                List<CategoriesResponse> allCategoriesResponse = new ArrayList<>();

                while (hasMore) {
                    String path = MessageFormat.format("/lists/{0}/interest-categories?count={1}&offset={2}",
                                                       task.getListId(),
                                                       count,
                                                       offset);
                    response = jsonParser.parseJsonObject(retryable.get(path));
                    InterestCategoriesResponse interestCategoriesResponse = mapper.treeToValue(response,
                                                                                               InterestCategoriesResponse.class);

                    allCategoriesResponse.addAll(interestCategoriesResponse.getCategories());
                    if (hasMorePage(interestCategoriesResponse.getTotalItems(), count, page)) {
                        offset = count;
                        page++;
                    }
                    else {
                        hasMore = false;
                    }
                }

                Function<CategoriesResponse, String> function = new Function<CategoriesResponse, String>()
                {
                    @Override
                    public String apply(CategoriesResponse input)
                    {
                        return input.getTitle().toLowerCase();
                    }
                };

                // Transform to a list of available category names and validate with data that user input
                ImmutableList<String> availableCategories = FluentIterable
                        .from(allCategoriesResponse)
                        .transform(function)
                        .toList();

                for (String category : interestCategoryNames) {
                    if (!availableCategories.contains(category)) {
                        throw new ConfigException("Invalid interest category name: '" + category + "'");
                    }
                }

                for (CategoriesResponse categoriesResponse : allCategoriesResponse) {
                    // Skip fetching interests if this category isn't specifed in the task's grouping column.
                    // Assume the grouping columns are always in lower case (like `availableCategories` did)
                    if (!interestCategoryNames.contains(categoriesResponse.getTitle().toLowerCase())) {
                        continue;
                    }

                    String detailPath = MessageFormat.format("/lists/{0}/interest-categories/{1}/interests",
                                                             task.getListId(),
                                                             categoriesResponse.getId());
                    response = jsonParser.parseJsonObject(retryable.get(detailPath));

                    // Avoid flood MailChimp API
                    avoidFloodAPI("Fetching next category's interests", task.getSleepBetweenRequestsMillis());
                    InterestsResponse interestsResponse = mapper.treeToValue(response, InterestsResponse.class);
                    categories.put(categoriesResponse.getTitle().toLowerCase(),
                                   convertInterestCategoryToMap(interestsResponse.getInterests()));
                }
            }

            // Warn if schema doesn't have the task's grouping column
            Set<String> columnNames = caseInsensitiveColumnNames(schema);
            Set<String> categoryNames = new HashSet<>(categories.keySet());
            if (!columnNames.containsAll(categoryNames)) {
                categoryNames.removeAll(columnNames);
                LOG.warn("Data schema doesn't contain the task's grouping column", on(", ").join(categoryNames));
            }

            return categories;
        }
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
                String path = MessageFormat.format("/lists/{0}/merge-fields?count={1}&offset={2}",
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
            jsonParser.parseJsonObject(retryable.get(MessageFormat.format("/lists/{0}",
                                                                          task.getListId())));
        }
        catch (HttpResponseException hre) {
            throw new ConfigException("The `list id` could not be found.");
        }
    }

    private Map<String, InterestResponse> convertInterestCategoryToMap(final List<InterestResponse> interestResponseList)
    {
        Function<InterestResponse, String> function = new Function<InterestResponse, String>()
        {
            @Override
            public String apply(@Nullable InterestResponse input)
            {
                return input.getName();
            }
        };

        return Maps.uniqueIndex(FluentIterable.from(interestResponseList)
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

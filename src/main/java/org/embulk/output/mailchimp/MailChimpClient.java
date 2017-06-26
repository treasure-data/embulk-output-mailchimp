package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.output.mailchimp.helper.MailChimpHelper;
import org.embulk.output.mailchimp.model.CategoriesResponse;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.InterestCategoriesResponse;
import org.embulk.output.mailchimp.model.InterestResponse;
import org.embulk.output.mailchimp.model.InterestsResponse;
import org.embulk.output.mailchimp.model.MergeField;
import org.embulk.output.mailchimp.model.MergeFields;
import org.embulk.output.mailchimp.model.MetaDataResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.output.mailchimp.model.AuthMethod.API_KEY;
import static org.embulk.output.mailchimp.model.AuthMethod.OAUTH;

/**
 * Created by thangnc on 4/25/17.
 */
public class MailChimpClient
{
    private static final Logger LOG = Exec.getLogger(MailChimpClient.class);
    private static final String API_VERSION = "3.0";
    private static String mailchimpEndpoint;
    private MailChimpHttpClient client;
    private final ObjectMapper mapper;

    /**
     * Instantiates a new Mail chimp client.
     *
     * @param task the task
     */
    public MailChimpClient(final MailChimpOutputPluginDelegate.PluginTask task)
    {
        mailchimpEndpoint = Joiner.on("/").join("https://{0}.api.mailchimp.com", API_VERSION);
        this.client = new MailChimpHttpClient(task);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false);
        extractDataCenter(task);
    }

    /**
     * Build an array of email subscribers and batch insert via bulk MailChimp API
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#create-post_lists_list_id
     *
     * @param node the data
     * @param task the task
     * @return the report response
     * @throws JsonProcessingException the json processing exception
     */
    ReportResponse push(final ObjectNode node, MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        String endpoint = MessageFormat.format(mailchimpEndpoint + "/lists/{0}",
                                               task.getListId());

        JsonNode response = client.sendRequest(endpoint, HttpMethod.POST, node.toString(), task);
        return mapper.treeToValue(response, ReportResponse.class);
    }

    /**
     * Handle detail errors after call bulk MailChimp API
     *
     * @param errorResponses the error responses
     */
    void handleErrors(List<ErrorResponse> errorResponses)
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
    Map<String, Map<String, InterestResponse>> extractInterestCategoriesByGroupNames(final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        Map<String, Map<String, InterestResponse>> categories = new HashMap<>();
        if (task.getGroupingColumns().isPresent() && !task.getGroupingColumns().get().isEmpty()) {
            List<String> interestCategoryNames = task.getGroupingColumns().get();

            String endpoint = MessageFormat.format(mailchimpEndpoint + "/lists/{0}/interest-categories",
                                                   task.getListId());

            JsonNode response = client.sendRequest(endpoint, HttpMethod.GET, task);
            InterestCategoriesResponse interestCategoriesResponse = mapper.treeToValue(response,
                                                                                       InterestCategoriesResponse.class);

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
                    .from(interestCategoriesResponse.getCategories())
                    .transform(function)
                    .toList();

            for (String category : interestCategoryNames) {
                if (!availableCategories.contains(category)) {
                    throw new ConfigException("Invalid interest category name: '" + category + "'");
                }
            }

            for (CategoriesResponse categoriesResponse : interestCategoriesResponse.getCategories()) {
                String detailEndpoint = MessageFormat.format(mailchimpEndpoint + "/lists/{0}/interest-categories/{1}/interests",
                                                             task.getListId(),
                                                             categoriesResponse.getId());
                response = client.sendRequest(detailEndpoint, HttpMethod.GET, task);
                InterestsResponse interestsResponse = mapper.treeToValue(response, InterestsResponse.class);
                categories.put(categoriesResponse.getTitle().toLowerCase(),
                               convertInterestCategoryToMap(interestsResponse.getInterests()));
            }
        }

        return categories;
    }

    /**
     * Extract merge fields from the list, find correct merge fields from API and put into the map to use
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/merge-fields/#read-get_lists_list_id_merge_fields
     *
     * @param task the task
     * @return the map
     * @throws JsonProcessingException the json processing exception
     */
    Map<String, MergeField> extractMergeFieldsFromList(MailChimpOutputPluginDelegate.PluginTask task) throws JsonProcessingException
    {
        String endpoint = MessageFormat.format(mailchimpEndpoint + "/lists/{0}/merge-fields",
                                               task.getListId());
        JsonNode response = client.sendRequest(endpoint, HttpMethod.GET, task);
        MergeFields mergeFields = mapper.treeToValue(response,
                                                     MergeFields.class);
        return convertMergeFieldToMap(mergeFields.getMergeFields());
    }

    private void extractDataCenter(MailChimpOutputPluginDelegate.PluginTask task)
    {
        try {
            if (task.getAuthMethod() == OAUTH) {
                // Extract data center from meta data URL
                JsonNode response = client.sendRequest("https://login.mailchimp.com/oauth2/metadata", HttpMethod.GET, task);
                MetaDataResponse metaDataResponse = mapper.treeToValue(response, MetaDataResponse.class);
                mailchimpEndpoint = MessageFormat.format(mailchimpEndpoint, metaDataResponse.getDc());
            }
            else if (task.getAuthMethod() == API_KEY && task.getApikey().isPresent()) {
                // Authenticate and return data center
                String domain = task.getApikey().get().split("-")[1];
                String endpoint = MessageFormat.format(mailchimpEndpoint + "/", domain);
                client.sendRequest(endpoint, HttpMethod.GET, task);
                mailchimpEndpoint = MessageFormat.format(mailchimpEndpoint, domain);
            }
        }
        catch (Exception e) {
            throw new ConfigException("Could not get data center", e);
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
}

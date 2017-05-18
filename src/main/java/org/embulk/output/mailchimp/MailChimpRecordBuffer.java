package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.output.mailchimp.helper.MailChimpHelper;
import org.embulk.output.mailchimp.model.CategoriesResponse;
import org.embulk.output.mailchimp.model.ErrorResponse;
import org.embulk.output.mailchimp.model.InterestCategoriesResponse;
import org.embulk.output.mailchimp.model.InterestResponse;
import org.embulk.output.mailchimp.model.InterestsResponse;
import org.embulk.output.mailchimp.model.MetaDataResponse;
import org.embulk.output.mailchimp.model.ReportResponse;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by thangnc on 4/25/17.
 */
public class MailChimpRecordBuffer extends MailChimpAbstractRecordBuffer
{
    private static final Logger LOG = Exec.getLogger(MailChimpRecordBuffer.class);
    private MailChimpHttpClient client;

    public MailChimpRecordBuffer(final Schema schema, final MailChimpOutputPluginDelegate.PluginTask task)
    {
        super(schema, task);
        client = new MailChimpHttpClient(task);
    }

    @Override
    void cleanUp()
    {
        client.close();
    }

    /**
     * Build an array of email subscribers and batch insert via bulk MailChimp API
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#create-post_lists_list_id
     *
     * @param node the data
     * @param task the task
     * @throws JsonProcessingException the json processing exception
     */
    @Override
    public ReportResponse push(final ObjectNode node, MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        String endpoint = MessageFormat.format(mailchimpEndpoint + "/3.0/lists/{0}",
                                               task.getListId());

        JsonNode response = client.sendRequest(endpoint, HttpMethod.POST, node.toString(), task);
        return getMapper().treeToValue(response, ReportResponse.class);
    }

    @Override
    void handleErrors(List<ErrorResponse> errorResponses)
    {
        if (!errorResponses.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder();

            for (ErrorResponse errorResponse : errorResponses) {
                errorMessage.append(MessageFormat.format("\nEmail `{0}` failed cause `{1}`",
                                                         MailChimpHelper.maskEmail(errorResponse.getEmailAddress()),
                                                         MailChimpHelper.maskEmail(errorResponse.getError())));
            }

            LOG.error(errorMessage.toString());
        }
    }

    Map<String, InterestResponse> fetchInterestCategoriesByGroupNames(final MailChimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        Map<String, InterestResponse> categories = new HashMap<>();
        if (task.getInterestCategories().isPresent() && !task.getInterestCategories().get().isEmpty()) {
            List<String> interestCategoryNames = task.getInterestCategories().get();

            String endpoint = MessageFormat.format(mailchimpEndpoint + "/3.0/lists/{0}/interest-categories",
                                                   task.getListId());

            JsonNode response = client.sendRequest(endpoint, HttpMethod.GET, task);
            InterestCategoriesResponse interestCategoriesResponse = getMapper().treeToValue(response,
                                                                                            InterestCategoriesResponse.class);
            for (CategoriesResponse categoriesResponse : interestCategoriesResponse.getCategories()) {
                String detailEndpoint = MessageFormat.format(mailchimpEndpoint + "/3.0/lists/{0}/interest-categories/{1}/interests",
                                                             task.getListId(),
                                                             categoriesResponse.getId());
                response = client.sendRequest(detailEndpoint, HttpMethod.GET, task);
                InterestsResponse interestsResponse = getMapper().treeToValue(response, InterestsResponse.class);
                categories.putAll(convertInterestCategoryToMap(interestCategoryNames, interestsResponse.getInterests()));
            }
        }

        return categories;
    }

    @Override
    String extractDataCenter(MailChimpOutputPluginDelegate.PluginTask task) throws JsonProcessingException
    {
        JsonNode response = client.sendRequest("https://login.mailchimp.com/oauth2/metadata", HttpMethod.GET, task);
        MetaDataResponse metaDataResponse = getMapper().treeToValue(response, MetaDataResponse.class);
        return metaDataResponse.getDc();
    }

    private Map<String, InterestResponse> convertInterestCategoryToMap(final List<String> interestCategoryNames,
                                                                       final List<InterestResponse> interestResponseList)
    {
        Predicate<InterestResponse> predicate = new Predicate<InterestResponse>()
        {
            @Override
            public boolean apply(@Nullable InterestResponse input)
            {
                return input != null && interestCategoryNames.contains(input.getName());
            }
        };

        Function<InterestResponse, String> function = new Function<InterestResponse, String>()
        {
            @Override
            public String apply(@Nullable InterestResponse input)
            {
                if (input != null && interestCategoryNames.contains(input.getName())) {
                    return input.getName().toLowerCase();
                }

                return "none";
            }
        };

        return Maps.uniqueIndex(FluentIterable.from(interestResponseList)
                                        .filter(predicate)
                                        .toList(),
                                function);
    }
}

package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailchimpHttpClient
{
    private static final Logger LOG = Exec.getLogger(MailchimpHttpClient.class);
    private static final String MAILCHIMP_API = "https://us15.api.mailchimp.com";
    private static final String API = "https://login.mailchimp.com";
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private Jetty92RetryHelper retryHelper;

    /**
     * Instantiates a new Mailchimp http client.
     *
     * @param task the task
     */
    public MailchimpHttpClient(MailchimpOutputPluginDelegate.PluginTask task)
    {
        retryHelper = createRetryHelper(task);
    }

    private JsonNode sendRequest(final String endpoint, final HttpMethod method,
                                 final MailchimpOutputPluginDelegate.PluginTask task)
    {
        return sendRequest(endpoint, method, "", task);
    }

    private JsonNode sendRequest(final String endpoint, final HttpMethod method, final String content,
                                 final MailchimpOutputPluginDelegate.PluginTask task)
    {
        final String authorizationHeader = getAuthorizationHeader(task);

        try {
            String responseBody = retryHelper.requestWithRetry(
                    new StringJetty92ResponseEntityReader(task.getTimeoutMills()),
                    new Jetty92SingleRequester()
                    {
                        @Override
                        public void requestOnce(HttpClient client, Response.Listener responseListener)
                        {
                            Request request = client
                                    .newRequest(endpoint)
                                    .accept("application/json")
                                    .method(method);
                            if (method == HttpMethod.POST || method == HttpMethod.PUT) {
                                request.content(new StringContentProvider(content), "application/json");
                            }

                            if (!authorizationHeader.isEmpty()) {
                                request.header("Authorization", authorizationHeader);
                            }
                            request.send(responseListener);
                        }

                        @Override
                        public boolean isResponseStatusToRetry(Response response)
                        {
                            int status = response.getStatus();
                            return status == 429 || status / 100 != 4;
                        }
                    });

            return responseBody != null && !responseBody.isEmpty() ? parseJson(responseBody) : MissingNode.getInstance();
        } catch (HttpResponseException ex) {
            LOG.error("Exception occurred while sending request: {}", ex.getMessage());

            throw ex;
        }
    }

    private JsonNode parseJson(final String json)
            throws DataException
    {
        try {
            return this.jsonMapper.readTree(json);
        } catch (IOException ex) {
            throw new DataException(ex);
        }
    }

    /**
     * MailChimp API v3 supports non expires access_token. Then no need refresh_token
     *
     * @param task
     * @return
     */
    private String getAuthorizationHeader(final MailchimpOutputPluginDelegate.PluginTask task)
    {
        switch (task.getAuthMethod()) {
            case OAUTH:
                return "OAuth " + task.getAccessToken();
            case API_KEY:
                return "Basic " + Base64Variants.MIME_NO_LINEFEEDS
                        .encode((RandomStringUtils.randomAlphabetic(10) + ":" + task.getApikey()).getBytes());
            default:
                throw new ConfigException("Not supported method");
        }
    }

    private Jetty92RetryHelper createRetryHelper(MailchimpOutputPluginDelegate.PluginTask task)
    {
        return new Jetty92RetryHelper(
                task.getMaximumRetries(),
                task.getInitialRetryIntervalMillis(),
                task.getMaximumRetryIntervalMillis(),
                new Jetty92ClientCreator()
                {
                    @Override
                    public HttpClient createAndStart()
                    {
                        HttpClient client = new HttpClient(new SslContextFactory());
                        try {
                            client.start();
                            return client;
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }

    /**
     * Build an array of email subscribers and batch insert via bulk MailChimp API
     * Reference: https://developer.mailchimp.com/documentation/mailchimp/reference/lists/#create-post_lists_list_id
     *
     * @param contactsData the contacts data
     * @param task         the task
     * @throws JsonProcessingException the json processing exception
     */
    public void push(final List<JsonNode> contactsData, MailchimpOutputPluginDelegate.PluginTask task)
            throws JsonProcessingException
    {
        LOG.info("Start to process subscribe data");
        String endpoint = MessageFormat.format(MAILCHIMP_API + "/3.0/lists/{0}/members",
                                               task.getListId());

        ArrayNode arrayOfEmailSubscribers = jsonMapper.createArrayNode();

        for (JsonNode contactData : contactsData) {
            ObjectNode property = jsonMapper.createObjectNode();
            property.put("email_address", contactData.findPath("email").asText());
            property.put("status", contactData.findPath("status").asText());
            arrayOfEmailSubscribers.add(property);
        }

        ObjectNode subscribers = jsonMapper.createObjectNode();
        subscribers.put("update_existing", task.isUpdateExisting());
        subscribers.putArray("members").addAll(arrayOfEmailSubscribers);

        String content = jsonMapper.writeValueAsString(subscribers);
        sendRequest(endpoint, HttpMethod.POST, content, task);
    }

    /**
     * Close @{@link Jetty92RetryHelper} connection
     */
    public void close()
    {
        retryHelper.close();
    }
}

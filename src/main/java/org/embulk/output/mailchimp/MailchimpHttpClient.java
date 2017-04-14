package org.embulk.output.mailchimp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailchimpHttpClient
{
    private static final Logger LOG = Exec.getLogger(MailchimpHttpClient.class);
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private String accessToken;

    private JsonNode sendRequest(final String endpoint, final HttpMethod method,
                                 final MailchimpOutputPluginDelegate.PluginTask task,
                                 final Jetty92RetryHelper retryHelper)
    {
        return sendRequest(endpoint, method, "", task, retryHelper);
    }

    private JsonNode sendRequest(final String endpoint, final HttpMethod method, final String content,
                                 final MailchimpOutputPluginDelegate.PluginTask task,
                                 final Jetty92RetryHelper retryHelper)
    {
        final String authorizationHeader = getAuthorizationHeader(retryHelper, task);

        try {
            String responseBody = retryHelper.requestWithRetry(
                    new StringJetty92ResponseEntityReader(task.getTimeoutMills()),
                    new Jetty92SingleRequester()
                    {
                        @Override
                        public void requestOnce(HttpClient client, Response.Listener responseListener)
                        {
                            org.eclipse.jetty.client.api.Request request = client
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
                        public boolean isResponseStatusToRetry(org.eclipse.jetty.client.api.Response response)
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

    private String getAuthorizationHeader(final Jetty92RetryHelper retryHelper,
                                          final MailchimpOutputPluginDelegate.PluginTask task)
    {
        try {
            String accessToken = retrieveAccessToken(retryHelper, task);
            return "Bearer " + accessToken;
        }
        catch (Exception e) {
            throw new ConfigException("Failed to refresh the access token: " + e);
        }
    }

    private String retrieveAccessToken(final Jetty92RetryHelper retryHelper,
                                       final MailchimpOutputPluginDelegate.PluginTask task)
    {
        if (this.accessToken != null) {
            return this.accessToken;
        }

        String responseBody = retryHelper.requestWithRetry(
                new StringJetty92ResponseEntityReader(task.getTimeoutMills()),
                new Jetty92SingleRequester()
                {
                    @Override
                    public void requestOnce(HttpClient client, Response.Listener responseListener)
                    {
                        final StringBuilder stringBuilder = new StringBuilder()
                                .append("code").append("=").append(task.getRefreshToken()).append("&")
                                .append("client_id").append("=").append(task.getClientId()).append("&")
                                .append("client_secret").append("=").append(task.getClientSecret()).append("&")
                                .append("redirect_uri").append("=").append("https://login.mailchimp.com/").append("&")
                                .append("grant_type").append("=").append("authorization_code");

                        Request request = client.newRequest("https://login.mailchimp.com/oauth2/token")
                                .method(HttpMethod.POST)
                                .content(new StringContentProvider(stringBuilder.toString()))
                                .header(HttpHeader.CONTENT_TYPE, "application/x-www-form-urlencoded");
                        request.send(responseListener);
                    }

                    @Override
                    protected boolean isResponseStatusToRetry(Response response)
                    {
                        return response.getStatus() / 100 != 4;
                    }
                }
        );

        this.accessToken = parseJson(responseBody).get("access_token").asText();
        return this.accessToken;
    }
}

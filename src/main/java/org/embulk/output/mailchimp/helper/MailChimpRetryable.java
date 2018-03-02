package org.embulk.output.mailchimp.helper;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.embulk.base.restclient.jackson.StringJsonParser;
import org.embulk.config.ConfigException;
import org.embulk.output.mailchimp.MailChimpOutputPluginDelegate.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.eclipse.jetty.http.HttpHeader.AUTHORIZATION;
import static org.eclipse.jetty.http.HttpMethod.GET;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.embulk.output.mailchimp.model.AuthMethod.API_KEY;
import static org.embulk.output.mailchimp.model.AuthMethod.OAUTH;

public class MailChimpRetryable implements AutoCloseable
{
    private static final Logger LOG = Exec.getLogger(MailChimpRetryable.class);
    private static final int READER_TIMEOUT_MILLIS = 300000;
    private static final String API_VERSION = "3.0";
    private final Jetty92RetryHelper retryHelper;
    private final PluginTask pluginTask;
    private static TokenHolder tokenHolder;
    protected StringJsonParser jsonParser = new StringJsonParser();
    private String authorizationHeader;

    public MailChimpRetryable(final PluginTask pluginTask)
    {
        this.retryHelper = new Jetty92RetryHelper(pluginTask.getMaximumRetries(),
                                                  pluginTask.getInitialRetryIntervalMillis(),
                                                  pluginTask.getMaximumRetryIntervalMillis(),
                                                  new DefaultJetty92ClientCreator(pluginTask.getTimeoutMillis(),
                                                                                  pluginTask.getTimeoutMillis()));
        this.pluginTask = pluginTask;
        authorizationHeader = buildAuthorizationHeader(pluginTask);
    }

    public String get(final String path)
    {
        return sendRequest(path, null);
    }

    public String post(final String path, String contentType, String body)
    {
        return sendRequest(path, new StringContentProvider(contentType, body, Charsets.UTF_8));
    }

    private String sendRequest(final String path, final StringContentProvider contentProvider)
    {
        return retryHelper.requestWithRetry(
                new StringJetty92ResponseEntityReader(READER_TIMEOUT_MILLIS),
                new Jetty92SingleRequester()
                {
                    @Override
                    public void requestOnce(HttpClient client, Response.Listener responseListener)
                    {
                        createTokenHolder(client);
                        Request request = client.newRequest(tokenHolder.getEndpoint() + path)
                                .header(AUTHORIZATION, authorizationHeader)
                                .method(GET);
                        if (contentProvider != null) {
                            request = request.method(POST).content(contentProvider);
                        }
                        request.send(responseListener);
                    }

                    @Override
                    protected boolean isResponseStatusToRetry(Response response)
                    {
                        // Retry if it's a server or rate limit exceeded error
                        return (response.getStatus() != 500 && response.getStatus() / 100 != 4) || response.getStatus() == 429;
                    }

                    @Override
                    protected boolean isExceptionToRetry(Exception exception)
                    {
                        // This check is to make sure if the original exception is retryable, i.e.
                        // server not found, internal server error...
                        if (exception instanceof ConfigException || exception instanceof ExecutionException) {
                            return toRetry((Exception) exception.getCause());
                        }
                        return exception instanceof TimeoutException || super.isExceptionToRetry(exception);
                    }
                });
    }

    @Override
    public void close()
    {
        if (retryHelper != null) {
            retryHelper.close();
        }
    }

    /**
     * MailChimp API v3 supports non expires access_token. Then no need refresh_token
     *
     * @param task
     * @return
     */
    private String buildAuthorizationHeader(final PluginTask task)
    {
        switch (task.getAuthMethod()) {
            case OAUTH:
                return "OAuth " + task.getAccessToken().orNull();
            case API_KEY:
                return "Basic " + Base64Variants.MIME_NO_LINEFEEDS
                        .encode(("apikey" + ":" + task.getApikey().orNull()).getBytes());
            default:
                throw new ConfigException("Not supported method");
        }
    }

    private TokenHolder createTokenHolder(final HttpClient client)
    {
        if (tokenHolder != null) {
            return tokenHolder;
        }

        LOG.info("Create new token holder and extract data center");

        if (pluginTask.getAuthMethod() == OAUTH) {
            try {
                // Extract data center from meta data URL
                ContentResponse contentResponse = client.newRequest("https://login.mailchimp.com/oauth2/metadata")
                        .method(GET)
                        .header("Authorization", authorizationHeader)
                        .send();

                if (contentResponse.getStatus() == 200) {
                    ObjectNode objectNode = jsonParser.parseJsonObject(contentResponse.getContentAsString());
                    String endpoint = MessageFormat.format(Joiner.on("/").join("https://{0}.api.mailchimp.com", API_VERSION),
                                                           objectNode.get("dc").asText());
                    tokenHolder = new TokenHolder(pluginTask.getAccessToken().orNull(), null, endpoint);
                    return tokenHolder;
                }

                String message = String.format("%s %d %s",
                                               contentResponse.getVersion(),
                                               contentResponse.getStatus(),
                                               contentResponse.getReason());
                throw new HttpResponseException(message, contentResponse);
            }
            catch (Exception ex) {
                throw new ConfigException("Unable to connect the data center", ex);
            }
        }
        else if (pluginTask.getAuthMethod() == API_KEY) {
            try {
                // Authenticate and return data center
                String domain = pluginTask.getApikey().get().split("-")[1];
                String endpoint = MessageFormat.format(Joiner.on("/").join("https://{0}.api.mailchimp.com", API_VERSION),
                                                       domain);
                ContentResponse contentResponse = client.newRequest(endpoint + "/")
                        .method(GET)
                        .header("Authorization", "Basic " + Base64Variants.MIME_NO_LINEFEEDS
                                .encode(("apikey" + ":" + pluginTask.getApikey().get()).getBytes()))
                        .send();

                if (contentResponse.getStatus() == 200) {
                    tokenHolder = new TokenHolder(null, pluginTask.getApikey().orNull(), endpoint);
                    return tokenHolder;
                }

                String message = String.format("%s %d %s",
                                               contentResponse.getVersion(),
                                               contentResponse.getStatus(),
                                               contentResponse.getReason());
                throw new HttpResponseException(message, contentResponse);
            }
            catch (Exception ex) {
                throw new ConfigException("Your API key may be invalid, or you've attempted to access the wrong datacenter.");
            }
        }

        throw new ConfigException("Not supported auth method");
    }
}

class TokenHolder
{
    private String accessToken;
    private String apiKey;
    private String endpoint;

    public TokenHolder(final String accessToken, final String apiKey, final String endpoint)
    {
        this.accessToken = accessToken;
        this.apiKey = apiKey;
        this.endpoint = endpoint;
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    public String getApiKey()
    {
        return apiKey;
    }

    public String getEndpoint()
    {
        return endpoint;
    }
}

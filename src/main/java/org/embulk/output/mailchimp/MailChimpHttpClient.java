package org.embulk.output.mailchimp;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Throwables;
import org.eclipse.jetty.client.HttpClient;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by thangnc on 4/14/17.
 */
public class MailChimpHttpClient
{
    private static final Logger LOG = Exec.getLogger(MailChimpHttpClient.class);
    private final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Instantiates a new Mailchimp http client.
     *
     * @param task the task
     */
    public MailChimpHttpClient(MailChimpOutputPluginDelegate.PluginTask task)
    {
    }

    public JsonNode sendRequest(final String endpoint, final HttpMethod method,
                                final MailChimpOutputPluginDelegate.PluginTask task)
    {
        return sendRequest(endpoint, method, "", task);
    }

    public JsonNode sendRequest(final String endpoint, final HttpMethod method, final String content,
                                final MailChimpOutputPluginDelegate.PluginTask task)
    {
        try (final Jetty92RetryHelper retryHelper = createRetryHelper(task)) {
            final String authorizationHeader = getAuthorizationHeader(task);

            String responseBody = retryHelper.requestWithRetry(
                    new StringJetty92ResponseEntityReader(task.getTimeoutMillis()),
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
                                request.content(new StringContentProvider(content), "application/json;utf-8");
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

                            if (status == 404) {
                                LOG.error("Exception occurred while sending request: {}", response.getReason());
                                throw new ConfigException("The `list id` could not be found.");
                            }

                            return status == 429 || status / 100 != 4;
                        }

                        @Override
                        protected boolean isExceptionToRetry(Exception exception)
                        {
                            // This check is to make sure the exception is retryable, e.g: server not found, internal server error...
                            if (exception instanceof ConfigException || exception instanceof ExecutionException) {
                                return toRetry((Exception) exception.getCause());
                            }

                            return exception instanceof TimeoutException || super.isExceptionToRetry(exception);
                        }
                    });

            return responseBody != null && !responseBody.isEmpty() ? parseJson(responseBody) : MissingNode.getInstance();
        }
        catch (Exception ex) {
            LOG.info("Exception occurred while sending request.");
            throw Throwables.propagate(ex);
        }
    }

    private JsonNode parseJson(final String json)
            throws DataException
    {
        try {
            return this.jsonMapper.readTree(json);
        }
        catch (IOException ex) {
            throw new DataException(ex);
        }
    }

    /**
     * MailChimp API v3 supports non expires access_token. Then no need refresh_token
     *
     * @param task
     * @return
     */
    private String getAuthorizationHeader(final MailChimpOutputPluginDelegate.PluginTask task)
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

    private Jetty92RetryHelper createRetryHelper(MailChimpOutputPluginDelegate.PluginTask task)
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
                        }
                        catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
    }
}

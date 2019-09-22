package org.embulk.output.mailchimp.helper;

import com.google.common.io.CharStreams;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * A copy of {@link org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader} with the only
 * modification is {@link PatchedStringJetty92ResponseEntityReader#getListener()} to return a new instance every time.
 * This might eventually get fixed upstream (Jetty92RetryHelper aware of Jetty92ResponseReader is stateful),
 */
public class PatchedStringJetty92ResponseEntityReader implements Jetty92ResponseReader<String>
{
    private InputStreamResponseListener listener;
    private final long timeoutMillis;

    public PatchedStringJetty92ResponseEntityReader(long timeoutMillis)
    {
        this.listener = new InputStreamResponseListener();
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public final Response.Listener getListener()
    {
        this.listener = new InputStreamResponseListener();
        return this.listener;
    }

    @Override
    public final Response getResponse() throws Exception
    {
        return this.listener.get(this.timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public final String readResponseContent() throws Exception
    {
        final InputStream inputStream = this.listener.getInputStream();
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream)) {
            return CharStreams.toString(inputStreamReader);
        }
    }

    @Override
    public final String readResponseContentInString() throws Exception
    {
        return this.readResponseContent();
    }
}

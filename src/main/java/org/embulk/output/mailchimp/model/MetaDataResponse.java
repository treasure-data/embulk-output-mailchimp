package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The POJO class of MailChimp v3 metadata
 * <p>
 * Created by thangnc on 5/16/17.
 */
public class MetaDataResponse
{
    private String dc;

    @JsonProperty("api_endpoint")
    private String apiEndpoint;

    public String getDc()
    {
        return dc;
    }

    public void setDc(String dc)
    {
        this.dc = dc;
    }

    public String getApiEndpoint()
    {
        return apiEndpoint;
    }

    public void setApiEndpoint(String apiEndpoint)
    {
        this.apiEndpoint = apiEndpoint;
    }
}

package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by thangnc on 5/8/17.
 */
public class InterestsResponse
{
    @JsonProperty("interests")
    private List<InterestResponse> interests;

    public List<InterestResponse> getInterests()
    {
        return interests;
    }

    public void setInterests(List<InterestResponse> interests)
    {
        this.interests = interests;
    }
}

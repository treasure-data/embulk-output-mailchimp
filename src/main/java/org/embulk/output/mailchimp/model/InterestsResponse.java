package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by thangnc on 5/8/17.
 */
public class InterestsResponse
{
    @JsonProperty("interests")
    private List<Interest> interests;

    @JsonProperty("total_items")
    private int totalItems;

    public List<Interest> getInterests()
    {
        return interests;
    }

    public void setInterests(List<Interest> interests)
    {
        this.interests = interests;
    }

    public int getTotalItems()
    {
        return totalItems;
    }

    public void setTotalItems(int totalItems)
    {
        this.totalItems = totalItems;
    }
}

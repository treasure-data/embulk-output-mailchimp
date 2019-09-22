package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by thangnc on 5/8/17.
 */
public class CategoriesResponse
{
    @JsonProperty("categories")
    private List<Category> categories;

    @JsonProperty("total_items")
    private int totalItems;

    public List<Category> getCategories()
    {
        return categories;
    }

    public void setCategories(List<Category> categories)
    {
        this.categories = categories;
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

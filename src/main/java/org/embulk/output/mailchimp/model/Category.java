package org.embulk.output.mailchimp.model;

/**
 * Created by thangnc on 5/5/17.
 */
public class Category
{
    private String id;
    private String title;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }
}

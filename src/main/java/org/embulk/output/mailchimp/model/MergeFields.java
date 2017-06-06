package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by thangnc on 5/8/17.
 */
public class MergeFields
{
    @JsonProperty("merge_fields")
    private List<MergeField> mergeFields;

    public List<MergeField> getMergeFields()
    {
        return mergeFields;
    }

    public void setMergeFields(List<MergeField> mergeFields)
    {
        this.mergeFields = mergeFields;
    }
}

package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The POJO class of MailChimp v3 response
 * <p>
 * Created by thangnc on 4/26/17.
 */
public class ReportResponse
{
    @JsonProperty("total_created")
    private int totalCreated;

    @JsonProperty("total_updated")
    private int totalUpdated;

    @JsonProperty("error_count")
    private int errorCount;

    @JsonProperty("errors")
    private List<ErrorResponse> errors;

    /**
     * The total number of created records matching the query, irrespective of pagination.
     *
     * @return the total created
     */
    public int getTotalCreated()
    {
        return totalCreated;
    }

    /**
     * Sets total created.
     *
     * @param totalCreated the total created
     */
    public void setTotalCreated(int totalCreated)
    {
        this.totalCreated = totalCreated;
    }

    /**
     * The total number of updated records matching the query, irrespective of pagination.
     *
     * @return the total updated
     */
    public int getTotalUpdated()
    {
        return totalUpdated;
    }

    /**
     * Sets total updated.
     *
     * @param totalUpdated the total updated
     */
    public void setTotalUpdated(int totalUpdated)
    {
        this.totalUpdated = totalUpdated;
    }

    /**
     * The total number of error records matching the query, irrespective of pagination.
     *
     * @return the error count
     */
    public int getErrorCount()
    {
        return errorCount;
    }

    /**
     * Sets error count.
     *
     * @param errorCount the error count
     */
    public void setErrorCount(int errorCount)
    {
        this.errorCount = errorCount;
    }

    /**
     * Gets errors.
     *
     * @return the errors
     */
    public List<ErrorResponse> getErrors()
    {
        return errors;
    }

    /**
     * Sets errors.
     *
     * @param errors the errors
     */
    public void setErrors(List<ErrorResponse> errors)
    {
        this.errors = errors;
    }
}

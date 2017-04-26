package org.embulk.output.mailchimp.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The POJO class of MailChimp v3 error. Each representing an email address that could not be
 * added to the list or updated and an error message providing more details.
 * <p>
 * Created by thangnc on 4/26/17.
 */
public class ErrorResponse
{
    @JsonProperty("email_address")
    private String emailAddress;
    private String error;

    /**
     * Gets email address.
     *
     * @return the email address
     */
    public String getEmailAddress()
    {
        return emailAddress;
    }

    /**
     * Sets email address.
     *
     * @param emailAddress the email address
     */
    public void setEmailAddress(String emailAddress)
    {
        this.emailAddress = emailAddress;
    }

    /**
     * Gets error.
     *
     * @return the error
     */
    public String getError()
    {
        return error;
    }

    /**
     * Sets error.
     *
     * @param error the error
     */
    public void setError(String error)
    {
        this.error = error;
    }
}

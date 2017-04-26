package org.embulk.output.mailchimp;

import org.junit.Test;

import java.util.Arrays;

import static org.embulk.output.mailchimp.helper.MailChimpHelper.containsCaseInsensitive;
import static org.embulk.output.mailchimp.helper.MailChimpHelper.maskEmail;
import static org.junit.Assert.assertEquals;

/**
 * Created by thangnc on 4/26/17.
 */
public class TestMailChimpHelper
{
    @Test
    public void test_maskEmail_validInErrorResponse()
    {
        String given = "thang0001@example.com looks fake or invalid, please enter a real email address.";
        String expect = "t******01@example.com looks fake or invalid, please enter a real email address.";
        assertEquals("Email should match", expect, maskEmail(given));
    }

    @Test
    public void test_containsCaseInsensitive_validMergeFields()
    {
        String expect = "FNAME";
        assertEquals("Merge field should match", expect, containsCaseInsensitive("fName",
                                                                                 Arrays.asList("FNAME", "LNAME")));
    }
}

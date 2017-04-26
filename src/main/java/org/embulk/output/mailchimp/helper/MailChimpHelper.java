package org.embulk.output.mailchimp.helper;

import java.util.List;

/**
 * Created by thangnc on 4/26/17.
 */
public final class MailChimpHelper
{
    private MailChimpHelper()
    {
    }

    /**
     * Mask email string.
     *
     * @param email the email
     * @return the string
     */
    public static String maskEmail(final String email)
    {
        return email.replaceAll("(?<=.).(?=[^@]*?..@)", "*");
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args)
    {
        String email = maskEmail("thang0001@example.com looks fake or invalid, please enter a real email address.");
        System.out.print(email);
    }

    /**
     * This method help to get explicit merge fields with column schema without case-sensitive
     *
     * @param s    the s
     * @param list the list
     * @return the boolean
     */
    public static String containsCaseInsensitive(final String s, final List<String> list)
    {
        for (String string : list) {
            if (string.equalsIgnoreCase(s)) {
                return string;
            }
        }

        return "";
    }
}

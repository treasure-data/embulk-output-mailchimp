package org.embulk.output.mailchimp.validation;

import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An Util class to validate data based on @{@link Schema} to build data payload
 * <p>
 * Created by thangnc on 4/18/17.
 */
public final class ColumnDataValidator
{
    private static final Logger LOG = Exec.getLogger(ColumnDataValidator.class);
    private static final Pattern VALID_EMAIL_ADDRESS_REGEX =
            Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$", Pattern.CASE_INSENSITIVE);
    private static final Pattern VALID_NAME_REGEX = Pattern.compile("^[a-z0-9_]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern VALID_WEBSITE_REGEX =
            Pattern.compile("^(http:\\/\\/|https:\\/\\/)?(www.)?([a-zA-Z0-9]+).[a-zA-Z0-9]*.[a-z]{3}.?([a-z]+)?$",
                            Pattern.CASE_INSENSITIVE);

    private ColumnDataValidator()
    {
    }

    /**
     * Is email address valid.
     *
     * @param email the email
     * @return the boolean
     */
    public static boolean isEmailAddressValid(final String email)
    {
        Matcher matcher = VALID_EMAIL_ADDRESS_REGEX.matcher(email);
        return matcher.find();
    }

    /**
     * Check required column name.
     *
     * @param schema             the schema
     * @param requiredColumnName the required column name
     * @return the boolean
     */
    public static boolean checkRequiredColumnName(final Schema schema, final String requiredColumnName)
    {
        int indexOfRequiredColumn = -1;

        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (schema.getColumnName(i).equals(requiredColumnName)) {
                indexOfRequiredColumn = i;
                break;
            }
        }

        return indexOfRequiredColumn != -1;
    }

    /**
     * Check required columns. In contact target, should require `email` and `status` columns
     *
     * @param schema  the schema
     * @param columns the columns
     * @return the boolean
     */
    public static boolean checkRequiredColumns(final Schema schema, final String... columns)
    {
        for (String column : columns) {
            if (!checkRequiredColumnName(schema, column)) {
                LOG.error("Column `{}` could not be found to create or update data.", column);
                return false;
            }
        }

        return true;
    }
}

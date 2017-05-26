package org.embulk.output.mailchimp.validation;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;

import java.util.Arrays;

/**
 * An Util class to validate data based on @{@link Schema} to build data payload
 * <p>
 * Created by thangnc on 4/18/17.
 */
public final class ColumnDataValidator
{
    private ColumnDataValidator()
    {
    }

    /**
     * Check required columns. Should require `email` and `status` columns
     *
     * @param schema       the schema
     * @param allowColumns the columns
     * @return the boolean
     */
    public static boolean checkExistColumns(final Schema schema, final String... allowColumns)
    {
        int found = 0;

        for (Column column : schema.getColumns()) {
            if (!Arrays.asList(allowColumns).contains(column.getName())) {
                continue;
            }

            found++;
        }

        return found == allowColumns.length;
    }
}

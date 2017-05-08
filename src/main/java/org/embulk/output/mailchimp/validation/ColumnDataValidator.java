package org.embulk.output.mailchimp.validation;

import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

/**
 * An Util class to validate data based on @{@link Schema} to build data payload
 * <p>
 * Created by thangnc on 4/18/17.
 */
public final class ColumnDataValidator
{
    private static final Logger LOG = Exec.getLogger(ColumnDataValidator.class);

    private ColumnDataValidator()
    {
    }

    /**
     * Check required column name.
     *
     * @param schema             the schema
     * @param requiredColumnName the required column name
     * @return the boolean
     */
    public static boolean checkExistColumnName(final Schema schema, final String requiredColumnName)
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
    public static boolean checkExistColumns(final Schema schema, final String... columns)
    {
        for (String column : columns) {
            if (!checkExistColumnName(schema, column)) {
                LOG.error("Column `{}` could not be found to create or update data.", column);
                return false;
            }
        }

        return true;
    }
}

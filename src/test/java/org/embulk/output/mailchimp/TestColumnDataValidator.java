package org.embulk.output.mailchimp;

import org.embulk.output.mailchimp.validation.ColumnDataValidator;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Test;

import static org.embulk.spi.type.Types.STRING;

/**
 * Created by thangnc on 5/12/17.
 */
public class TestColumnDataValidator
{
    @Test
    public void test_checkExistColumns_valid()
    {
        Schema schema = Schema.builder()
                .add("email", STRING)
                .add("fname", STRING)
                .add("lname", STRING)
                .add("status", STRING)
                .build();

        Assert.assertEquals("Column should be exists",
                            true,
                            ColumnDataValidator.checkExistColumns(schema, "status", "email"));
    }

    @Test
    public void test_checkExistColumns_invalid()
    {
        Schema schema = Schema.builder()
                .add("email", STRING)
                .add("fname", STRING)
                .add("lname", STRING)
                .build();

        Assert.assertEquals("Column should be not exist",
                            false,
                            ColumnDataValidator.checkExistColumns(schema, "status", "email"));
    }
}

package org.embulk.output.mailchimp;

import org.embulk.output.mailchimp.validation.ColumnDataValidator;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by thangnc on 5/12/17.
 */
public class TestColumnDataValidator
{
    @Test
    public void test_checkExistColumns_valid()
    {
        Schema schema = Schema.builder()
                .add("email", org.embulk.spi.type.Types.STRING)
                .add("fname", org.embulk.spi.type.Types.STRING)
                .add("lname", org.embulk.spi.type.Types.STRING)
                .add("status", org.embulk.spi.type.Types.STRING)
                .build();

        Assert.assertEquals("Column should be exists",
                            true,
                            ColumnDataValidator.checkExistColumns(schema, "status", "email"));
    }

    @Test
    public void test_checkExistColumns_invalid()
    {
        Schema schema = Schema.builder()
                .add("email", org.embulk.spi.type.Types.STRING)
                .add("fname", org.embulk.spi.type.Types.STRING)
                .add("lname", org.embulk.spi.type.Types.STRING)
                .build();

        Assert.assertEquals("Column should be not exist",
                            false,
                            ColumnDataValidator.checkExistColumns(schema, "status", "email"));
    }
}

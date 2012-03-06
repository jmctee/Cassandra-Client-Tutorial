package com.jeklsoft.cassandraclient.astyanax;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.netflix.astyanax.Serializer;

public class TestBigDecimalSerializer {
    private static final Serializer<BigDecimal> serializer = BigDecimalSerializer.get();

    @Test
    public void SerializingThenDeserializingBigDecimalResultsInSameBigDecimal() {
        BigDecimal value = new BigDecimal(1.3);

        ByteBuffer buffer = serializer.toByteBuffer(value);

        BigDecimal deserializedBigDecimal = serializer.fromByteBuffer(buffer);

        assertEquals(value, deserializedBigDecimal);
    }
}

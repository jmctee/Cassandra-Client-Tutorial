package com.jeklsoft.cassandraclient;

import com.jeklsoft.cassandraclient.serializer.hector.BigDecimalSerializer;
import com.jeklsoft.cassandraclient.serializer.hector.ExtensibleTypeInferrringSerializer;
import me.prettyprint.hector.api.Serializer;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestBigDecimalSerializer {
    @Test
    public void BigDecimalObjectShouldReturnBigDecimalSerializer() {
        ExtensibleTypeInferrringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
        BigDecimal value = new BigDecimal(1);
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BigDecimalSerializer.class);
    }

    @Test
    public void BigDecimalClassShouldReturnBigDecimalSerializer() {
        ExtensibleTypeInferrringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(BigDecimal.class);
        assertEquals(serializer.getClass(), BigDecimalSerializer.class);
    }

    @Test
    public void SerializingThenDeserializingBigDecimalResultsInSameBigDecimal() {
        ExtensibleTypeInferrringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
        BigDecimal value = new BigDecimal(1);

        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(BigDecimal.class);

        ByteBuffer buffer = serializer.toByteBuffer(value);

        BigDecimal deserializedBigDecimal = (BigDecimal) serializer.fromByteBuffer(buffer);

        assertEquals(value, deserializedBigDecimal);
    }
}

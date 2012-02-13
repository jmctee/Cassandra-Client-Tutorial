package com.jeklsoft.hector;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import me.prettyprint.hector.api.Serializer;

import org.joda.time.DateTime;

import java.nio.ByteBuffer;

public class TestDateTimeSerializer {
    @Test
    public void DateTimeObjectShouldReturnDateTimeSerializer()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        DateTime value = new DateTime();
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DateTimeSerializer.class);
    }

    @Test
    public void DateTimeClassShouldReturnDateTimeSerializer()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(DateTime.class);
        assertEquals(serializer.getClass(), DateTimeSerializer.class);
    }

    @Test
    public void SerializingThenDeserializingDateTimeResultsInSameDateTime()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        DateTime value = new DateTime(2000, 1, 1, 12, 0);

        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(DateTime.class);

        ByteBuffer buffer = serializer.toByteBuffer(value);

        DateTime deserializedDateTime = (DateTime) serializer.fromByteBuffer(buffer);

        assertEquals(value, deserializedDateTime);
    }
}
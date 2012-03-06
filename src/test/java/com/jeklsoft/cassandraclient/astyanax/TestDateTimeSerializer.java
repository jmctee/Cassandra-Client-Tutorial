package com.jeklsoft.cassandraclient.astyanax;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.joda.time.DateTime;
import org.junit.Test;

import com.netflix.astyanax.Serializer;

public class TestDateTimeSerializer {
    private static final Serializer<DateTime> serializer = DateTimeSerializer.get();

    @Test
    public void SerializingThenDeserializingDateTimeResultsInSameDateTime() {
        DateTime value = new DateTime(2000, 1, 1, 12, 0);

        ByteBuffer buffer = serializer.toByteBuffer(value);

        DateTime deserializedDateTime = serializer.fromByteBuffer(buffer);

        assertEquals(value, deserializedDateTime);
    }
}

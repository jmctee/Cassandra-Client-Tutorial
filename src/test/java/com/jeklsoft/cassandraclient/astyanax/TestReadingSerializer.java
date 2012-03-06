package com.jeklsoft.cassandraclient.astyanax;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.joda.time.DateTime;
import org.junit.Test;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.serializer.astyanax.ReadingSerializer;
import com.netflix.astyanax.Serializer;

public class TestReadingSerializer {

    private final static Serializer serializer = ReadingSerializer.get();

    @Test
    public void SerializingThenDeserializingReadingResultsInSameReading() {
        Reading reading = new Reading(UUID.randomUUID(), new DateTime(), new BigDecimal(195).movePointLeft(1),
                24, "ESE", BigInteger.valueOf(17L), false);

        ByteBuffer buffer = serializer.toByteBuffer(reading);

        Reading deserializedReading = (Reading) serializer.fromByteBuffer(buffer);

        assertEquals(reading, deserializedReading);
    }
}

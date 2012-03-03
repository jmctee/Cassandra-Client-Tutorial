package com.jeklsoft.hector;

import me.prettyprint.hector.api.Serializer;
import org.joda.time.DateTime;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestReadingSerializer {
    @Test
    public void ReadingObjectShouldReturnReadingSerializer()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
        Reading reading = new Reading(UUID.randomUUID(), new DateTime(), new BigDecimal(195).movePointLeft(1),
                24, "ESE", BigInteger.valueOf(17L), false);
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(reading);
        assertEquals(serializer.getClass(), ReadingSerializer.class);
    }

    @Test
    public void ReadingClassShouldReturnReadingSerializer()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Reading.class);
        assertEquals(serializer.getClass(), ReadingSerializer.class);
    }

    @Test
    public void SerializingThenDeserializingReadingResultsInSameReading()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
        Reading reading = new Reading(UUID.randomUUID(), new DateTime(), new BigDecimal(195).movePointLeft(1),
                24, "ESE", BigInteger.valueOf(17L), false);

        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Reading.class);

        ByteBuffer buffer = serializer.toByteBuffer(reading);

        Reading deserializedReading = (Reading) serializer.fromByteBuffer(buffer);

        assertEquals(reading, deserializedReading);
    }
}

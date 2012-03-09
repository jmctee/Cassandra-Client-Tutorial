//Copyright 2012 Joe McTee
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.jeklsoft.cassandraclient.hector;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.joda.time.DateTime;
import org.junit.Test;

import com.jeklsoft.cassandraclient.Reading;

import me.prettyprint.hector.api.Serializer;

public class TestReadingSerializer {
    @Test
    public void ReadingObjectShouldReturnReadingSerializer() {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
        Reading reading = new Reading(UUID.randomUUID(), new DateTime(), new BigDecimal(195).movePointLeft(1),
                24, "ESE", BigInteger.valueOf(17L), false);
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(reading);
        assertEquals(serializer.getClass(), ReadingSerializer.class);
    }

    @Test
    public void ReadingClassShouldReturnReadingSerializer() {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Reading.class);
        assertEquals(serializer.getClass(), ReadingSerializer.class);
    }

    @Test
    public void SerializingThenDeserializingReadingResultsInSameReading() {
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());

        UUID uuid = UUID.randomUUID();
        DateTime date = new DateTime();

        Reading reading = new Reading(uuid, date, new BigDecimal(195).movePointLeft(1),
                24, "ESE", BigInteger.valueOf(17L), false);

        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Reading.class);

        ByteBuffer buffer = serializer.toByteBuffer(reading);

        Reading deserializedReading = new Reading(uuid, date, (Reading) serializer.fromByteBuffer(buffer));

        assertEquals(reading, deserializedReading);
    }
}

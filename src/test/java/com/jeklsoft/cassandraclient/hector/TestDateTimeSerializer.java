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

import java.nio.ByteBuffer;

import org.joda.time.DateTime;
import org.junit.Test;

import me.prettyprint.hector.api.Serializer;

public class TestDateTimeSerializer {
    @Test
    public void DateTimeObjectShouldReturnDateTimeSerializer() {
        ExtensibleTypeInferringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        DateTime value = new DateTime();
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DateTimeSerializer.class);
    }

    @Test
    public void DateTimeClassShouldReturnDateTimeSerializer() {
        ExtensibleTypeInferringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(DateTime.class);
        assertEquals(serializer.getClass(), DateTimeSerializer.class);
    }

    @Test
    public void SerializingThenDeserializingDateTimeResultsInSameDateTime() {
        ExtensibleTypeInferringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        DateTime value = new DateTime(2000, 1, 1, 12, 0);

        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(DateTime.class);

        ByteBuffer buffer = serializer.toByteBuffer(value);

        DateTime deserializedDateTime = (DateTime) serializer.fromByteBuffer(buffer);

        assertEquals(value, deserializedDateTime);
    }
}

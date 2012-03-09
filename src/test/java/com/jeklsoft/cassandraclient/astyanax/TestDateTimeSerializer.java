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

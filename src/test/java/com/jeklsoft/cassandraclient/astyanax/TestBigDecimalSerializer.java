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

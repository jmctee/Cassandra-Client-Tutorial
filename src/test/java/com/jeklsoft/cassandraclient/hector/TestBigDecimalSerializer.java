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
import java.nio.ByteBuffer;

import org.junit.Test;

import me.prettyprint.hector.api.Serializer;

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

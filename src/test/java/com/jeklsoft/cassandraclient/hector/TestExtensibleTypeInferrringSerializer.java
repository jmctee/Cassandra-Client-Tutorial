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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.ShortSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Serializer;

public class TestExtensibleTypeInferrringSerializer {

    @Test
    public void uuidObjectShouldReturnUUIDSerializer() {
        UUID value = new UUID(0, 1);
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), UUIDSerializer.class);
    }

    @Test
    public void uuidClassShouldReturnUUIDSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(UUID.class);
        assertEquals(serializer.getClass(), UUIDSerializer.class);
    }

    @Test
    public void stringObjectShouldReturnStringSerializer() {
        String value = "test";
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), StringSerializer.class);
    }

    @Test
    public void stringClassShouldReturnStringSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(String.class);
        assertEquals(serializer.getClass(), StringSerializer.class);
    }

    @Test
    public void longValueShouldReturnLongSerializer() {
        long value = 1L;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    @Test
    public void longObjectShouldReturnLongSerializer() {
        Long value = 1L;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    @Test
    public void longClassShouldReturnLongSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Long.class);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    @Test
    public void intValueShouldReturnIntegerSerializer() {
        int value = 1;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    @Test
    public void intObjectShouldReturnIntegerSerializer() {
        Integer value = 1;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    @Test
    public void intClassShouldReturnIntegerSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Integer.class);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    @Test
    public void booleanValueShouldReturnBooleanSerializer() {
        boolean value = true;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    @Test
    public void booleanObjectShouldReturnBooleanSerializer() {
        Boolean value = true;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    @Test
    public void booleanClassShouldReturnBooleanSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Boolean.class);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    @Test
    public void doubleValueShouldReturnDoubleSerializer() {
        double value = 1.0;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    @Test
    public void doubleObjectShouldReturnDoubleSerializer() {
        Double value = 1.0;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    @Test
    public void doubleClassShouldReturnDoubleSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Double.class);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    @Test
    public void floatValueShouldReturnFloatSerializer() {
        float value = 1.0F;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    @Test
    public void floatObjectShouldReturnFloatSerializer() {
        Float value = 1.0F;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    @Test
    public void floatClassShouldReturnFloatSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Float.class);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    @Test
    public void shortValueShouldReturnShortSerializer() {
        short value = 1;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    @Test
    public void shortObjectShouldReturnShortSerializer() {
        Short value = 1;
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    @Test
    public void shortClassShouldReturnShortSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Short.class);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    @Test
    public void byteArrayObjectShouldReturnBytesArraySerializer() {
        byte[] value = {1, 2, 3};
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BytesArraySerializer.class);
    }

    @Test
    public void byteArrayClassShouldReturnBytesArraySerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(byte[].class);
        assertEquals(serializer.getClass(), BytesArraySerializer.class);
    }

    @Test
    public void byteBufferObjectShouldReturnByteBufferSerializer() {
        ByteBuffer value = ByteBuffer.allocate(1);
        value.put(new Integer(1).byteValue());
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    @Test
    public void byteBufferClassShouldReturnByteBufferSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(ByteBuffer.class);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    @Test
    public void dateObjectShouldReturnDateSerializer() {
        Date value = new Date();
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DateSerializer.class);
    }

    @Test
    public void dateClassShouldReturnDateSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Date.class);
        assertEquals(serializer.getClass(), DateSerializer.class);
    }

    @Test
    public void bigIntegerObjectShouldReturnBigIntegerSerializer() {
        BigInteger value = BigInteger.valueOf(1);
        ExtensibleTypeInferringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BigIntegerSerializer.class);
    }

    @Test
    public void bigIntegerClassShouldReturnBigIntegerSerializer() {
        ExtensibleTypeInferringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(BigInteger.class);
        assertEquals(serializer.getClass(), BigIntegerSerializer.class);
    }

    @Test
    public void objectObjectShouldReturnObjectSerializer() {
        Object value = new Object();
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @Test
    public void objectClassShouldReturnObjectSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(Object.class);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void unsupportedClassObjectShouldReturnObjectSerializer() {
        List value = new ArrayList();
        value.add(1);
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @Test
    public void unsupportedClassShouldReturnObjectSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(ArrayList.class);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @Test
    public void nullObjectShouldReturnByteBufferSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer((Object) null);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    @SuppressWarnings({"NullableProblems"})
    @Test
    public void nullClassShouldReturnByteBufferSerializer() {
        Serializer serializer = ExtensibleTypeInferringSerializer.getSerializer(null);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }
}

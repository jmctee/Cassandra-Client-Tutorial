package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.ShortSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.hector.api.Serializer;

public class TestExtensibleTypeInferrringSerializer {
    //    UUID
    @Test
    public void uuidObjectShouldReturnUUIDSerializer()
    {
        UUID value = new UUID(0,1);
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), UUIDSerializer.class);
    }

    @Test
    public void uuidClassShouldReturnUUIDSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(UUID.class);
        assertEquals(serializer.getClass(), UUIDSerializer.class);
    }

    //    String
    @Test
    public void stringObjectShouldReturnStringSerializer()
    {
        String value = "test";
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), StringSerializer.class);
    }

    @Test
    public void stringClassShouldReturnStringSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(String.class);
        assertEquals(serializer.getClass(), StringSerializer.class);
    }

    //    Long/long
    @Test
    public void longValueShouldReturnLongSerializer()
    {
        long value = 1L;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    @Test
    public void longObjectShouldReturnLongSerializer()
    {
        Long value = 1L;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    @Test
    public void longClassShouldReturnLongSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Long.class);
        assertEquals(serializer.getClass(), LongSerializer.class);
    }

    //    Integer/int
    @Test
    public void intValueShouldReturnIntegerSerializer()
    {
        int value = 1;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    @Test
    public void intObjectShouldReturnIntegerSerializer()
    {
        Integer value = 1;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    @Test
    public void intClassShouldReturnIntegerSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Integer.class);
        assertEquals(serializer.getClass(), IntegerSerializer.class);
    }

    //    Boolean/boolean
    @Test
    public void booleanValueShouldReturnBooleanSerializer()
    {
        boolean value = true;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    @Test
    public void booleanObjectShouldReturnBooleanSerializer()
    {
        Boolean value = true;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    @Test
    public void booleanClassShouldReturnBooleanSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Boolean.class);
        assertEquals(serializer.getClass(), BooleanSerializer.class);
    }

    //    Double/double
    @Test
    public void doubleValueShouldReturnDoubleSerializer()
    {
        double value = 1.0;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    @Test
    public void doubleObjectShouldReturnDoubleSerializer()
    {
        Double value = 1.0;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    @Test
    public void doubleClassShouldReturnDoubleSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Double.class);
        assertEquals(serializer.getClass(), DoubleSerializer.class);
    }

    //    Float/float
    @Test
    public void floatValueShouldReturnFloatSerializer()
    {
        float value = 1.0F;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    @Test
    public void floatObjectShouldReturnFloatSerializer()
    {
        Float value = 1.0F;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    @Test
    public void floatClassShouldReturnFloatSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Float.class);
        assertEquals(serializer.getClass(), FloatSerializer.class);
    }

    //    Short/short
    @Test
    public void shortValueShouldReturnShortSerializer()
    {
        short value = 1;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    @Test
    public void shortObjectShouldReturnShortSerializer()
    {
        Short value = 1;
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    @Test
    public void shortClassShouldReturnShortSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Short.class);
        assertEquals(serializer.getClass(), ShortSerializer.class);
    }

    //    byte[]
    @Test
    public void byteArrayObjectShouldReturnBytesArraySerializer()
    {
        byte[] value = {1,2,3};
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BytesArraySerializer.class);
    }

    @Test
    public void byteArrayClassShouldReturnBytesArraySerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(byte[].class);
        assertEquals(serializer.getClass(), BytesArraySerializer.class);
    }

    //    ByteBuffer
    @Test
    public void byteBufferObjectShouldReturnByteBufferSerializer()
    {
        ByteBuffer value = ByteBuffer.allocate(1);
        value.put(new Integer(1).byteValue());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    @Test
    public void byteBufferClassShouldReturnByteBufferSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(ByteBuffer.class);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    //    Date
    @Test
    public void dateObjectShouldReturnDateSerializer()
    {
        Date value = new Date();
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), DateSerializer.class);
    }

    @Test
    public void dateClassShouldReturnDateSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Date.class);
        assertEquals(serializer.getClass(), DateSerializer.class);
    }

    //    BigInteger
    @Test
    public void bigIntegerObjectShouldReturnBigIntegerSerializer()
    {
        BigInteger value = BigInteger.valueOf(1);
        ExtensibleTypeInferrringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), BigIntegerSerializer.class);
    }

    @Test
    public void bigIntegerClassShouldReturnBigIntegerSerializer()
    {
        ExtensibleTypeInferrringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(BigInteger.class);
        assertEquals(serializer.getClass(), BigIntegerSerializer.class);
    }

    //    Object
    @Test
    public void objectObjectShouldReturnObjectSerializer()
    {
        Object value = new Object();
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @Test
    public void objectClassShouldReturnObjectSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(Object.class);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    //    Unsupported class
    @SuppressWarnings({"unchecked"})
    @Test
    public void unsupportedClassObjectShouldReturnObjectSerializer()
    {
        List value = new ArrayList();
        value.add(1);
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(value);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    @Test
    public void unsupportedClassShouldReturnObjectSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(ArrayList.class);
        assertEquals(serializer.getClass(), ObjectSerializer.class);
    }

    //    null
    @Test
    public void nullObjectShouldReturnByteBufferSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer((Object) null);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }

    @SuppressWarnings({"NullableProblems"})
    @Test
    public void nullClassShouldReturnByteBufferSerializer()
    {
        Serializer serializer = ExtensibleTypeInferrringSerializer.getSerializer(null);
        assertEquals(serializer.getClass(), ByteBufferSerializer.class);
    }
}

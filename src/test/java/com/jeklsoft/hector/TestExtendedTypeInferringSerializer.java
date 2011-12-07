package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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

public class TestExtendedTypeInferringSerializer {

    private static ExtendedTypeInferringSerializer extendedTypeInferringSerializer;

    @BeforeClass
    public static void setup() throws Exception {
        extendedTypeInferringSerializer = ExtendedTypeInferringSerializer.get();
    }

    //    UUID
    @Test
    public void uuidObjectShouldReturnUUIDByteBuffer()
    {
        UUID value = new UUID(0x0807060504030201L, 0x0102030405060708L);
        long msb = value.getMostSignificantBits();
        long lsb = value.getLeastSignificantBits();
        byte[] array = new byte[16];

        for (int ii = 0; ii < 8; ii++) {
          int shiftCount = 8 * (7 - ii);
          array[ii] = (byte) (msb >>> shiftCount);
          array[ii + 8] = (byte) (lsb >>> shiftCount);
        }

        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    String
    @Test
    public void stringObjectShouldReturnStringByteBuffer()
    {
        String value = "Test";
        byte[] array = value.getBytes();

        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Long
    @Test
    public void longObjectShouldReturnLongByteBuffer()
    {
        Long value = new Long(0x0807060504030201L);

        byte[] array = createByteArrayFromLong(value, 8);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Integer
    @Test
    public void integerObjectShouldReturnIntegerByteBuffer()
    {
        Integer value = new Integer(0x04030201);

        byte[] array = createByteArrayFromLong(new Long(value), 4);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Boolean
    @Test
    public void booleanObjectShouldReturnBooleanByteBuffer()
    {
        Boolean value = true;

        byte[] array = createByteArrayFromLong(1L, 1);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);

        value = false;

        array = createByteArrayFromLong(0L, 1);
        expectedBuffer = ByteBuffer.wrap(array);

        buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Double
    @Test
    public void doubleObjectShouldReturnDoubleByteBuffer()
    {
        Double value = 1.234;

        Long intBits = Double.doubleToRawLongBits(value);
        byte[] array = createByteArrayFromLong(intBits, 8);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Float
    @Test
    public void floatObjectShouldReturnFloatByteBuffer()
    {
        Float value = 1.234F;

        Long intBits = new Long(Float.floatToRawIntBits(value));
        byte[] array = createByteArrayFromLong(intBits, 4);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Short
    @Test
    public void shortObjectShouldReturnShortByteBuffer()
    {
        Short value = new Integer(1).shortValue();

        byte[] array = createByteArrayFromLong(new Long(value), 2);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    byte[]
    @Test
    public void byteArrayObjectShouldReturnBytesArrayByteBuffer()
    {
        byte[] value = new byte[3];
        value[0] = 1;
        value[1] = 2;
        value[2] = 3;

        ByteBuffer expectedBuffer = ByteBuffer.wrap(value);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    ByteBuffer
    @Test
    public void byteBufferObjectShouldReturnByteBufferByteBuffer()
    {
        byte[] array = new byte[3];
        array[0] = 1;
        array[1] = 2;
        array[2] = 3;

        ByteBuffer value = ByteBuffer.wrap(array);

        byte[] array2 = new byte[3];
        array2[0] = 1;
        array2[1] = 2;
        array2[2] = 3;

        ByteBuffer expectedBuffer = ByteBuffer.wrap(array2);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Date
    @Test
    public void dateObjectShouldReturnDateByteBuffer()
    {
        Date value = new Date();

        byte[] array = createByteArrayFromLong(value.getTime(), 8);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    BigInteger
    @Test
    public void bigIntegerObjectShouldReturnBigIntegerByteBuffer()
    {
        BigInteger value = BigInteger.valueOf(0x0807060504030201L);

        byte[] array = value.toByteArray();
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    Object
    @Test
    public void serializableObjectObjectShouldReturnObjectByteBuffer() throws IOException
    {
        // ArrayList is serializable
        List list = new ArrayList();
        list.add(1);
        list.add(2);
        list.add(3);

        Object value = list;

        byte[] array = createByteArrayFromObject(list);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    @Test (expected = NotSerializableException.class)
    public void nonserializableObjectObjectShouldThrowException() throws IOException
    {
        // Object is not serializable
        Object value = new Object();

        byte[] array = createByteArrayFromObject(value);
        ByteBuffer expectedBuffer = ByteBuffer.wrap(array);

        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(value);

        assertEquals(expectedBuffer, buffer);
    }

    //    null
    @Test
    public void nullObjectShouldReturnNull()
    {
        ByteBuffer buffer = extendedTypeInferringSerializer.toByteBuffer(null);

        assertEquals(null, buffer);
    }

    // FromByteBuffer throws exception
    @Test (expected=IllegalStateException.class)
    public void callingFromByteBufferThrowsException()
    {
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(new Integer(0).byteValue());
        buffer.put(new Integer(1).byteValue());
        buffer.put(new Integer(2).byteValue());

        Object value = extendedTypeInferringSerializer.fromByteBuffer(buffer);
    }

    private byte[] createByteArrayFromLong(Long value, int bytesToUse)
    {
        byte[] array = new byte[bytesToUse];

        for (int ii = 0; ii < bytesToUse; ii++) {
          int shiftCount = 8 * (bytesToUse - 1 - ii);
          array[ii] = (byte) (value >>> shiftCount);
        }

        return array;
    }

    private byte[] createByteArrayFromObject(Object obj) throws IOException
    {
      // Only works if object is serializable, otherwise exception...
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(obj);
      objectOutputStream.close();
      return byteArrayOutputStream.toByteArray();
    }

}

package com.jeklsoft.hector;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.serializers.FloatSerializer;
import me.prettyprint.cassandra.serializers.ShortSerializer;
import me.prettyprint.hector.api.Serializer;

public class ExtendedSerializerTypeInferer {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <T> Serializer<T> getSerializer(Object value) {
    Serializer serializer = (value == null) ? ByteBufferSerializer.get() : getSerializer(value.getClass());
    return serializer;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
    Serializer serializer;

    if (valueClass == null) {
      serializer = ByteBufferSerializer.get();
    } else if (valueClass.equals(UUID.class)) {
      serializer = UUIDSerializer.get();
    } else if (valueClass.equals(String.class)) {
      serializer = StringSerializer.get();
    } else if (valueClass.equals(Long.class) || valueClass.equals(long.class)) {
      serializer = LongSerializer.get();
    } else if (valueClass.equals(Integer.class) || valueClass.equals(int.class)) {
      serializer = IntegerSerializer.get();
    } else if (valueClass.equals(Boolean.class) || valueClass.equals(boolean.class)) {
      serializer = BooleanSerializer.get();
    } else if (valueClass.equals(Double.class) || valueClass.equals(double.class)) {
      serializer = DoubleSerializer.get();
    } else if (valueClass.equals(Float.class) || valueClass.equals(float.class)) {
      serializer = FloatSerializer.get();
    } else if (valueClass.equals(Short.class) || valueClass.equals(short.class)) {
      serializer = ShortSerializer.get();
    } else if (valueClass.equals(byte[].class)) {
      serializer = BytesArraySerializer.get();
    } else if (ByteBuffer.class.isAssignableFrom(valueClass)) {
      serializer = ByteBufferSerializer.get();
    } else if (valueClass.equals(Date.class)) {
      serializer = DateSerializer.get();
    } else if (valueClass.equals(BigInteger.class)) {
      serializer = BigIntegerSerializer.get();
    } else {
      serializer = ObjectSerializer.get();
    }

    return serializer;
  }
}

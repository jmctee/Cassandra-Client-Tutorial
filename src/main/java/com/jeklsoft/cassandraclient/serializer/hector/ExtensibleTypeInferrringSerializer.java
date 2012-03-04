package com.jeklsoft.cassandraclient.serializer.hector;


import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.hector.api.Serializer;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ExtensibleTypeInferrringSerializer {

    private static Map<Class<?>, AbstractSerializer> serializers = new HashMap<Class<?>, AbstractSerializer>();

    static {
        serializers.put(UUID.class, UUIDSerializer.get());
        serializers.put(String.class, StringSerializer.get());
        serializers.put(Long.class, LongSerializer.get());
        serializers.put(long.class, LongSerializer.get());
        serializers.put(Integer.class, IntegerSerializer.get());
        serializers.put(int.class, IntegerSerializer.get());
        serializers.put(Short.class, ShortSerializer.get());
        serializers.put(short.class, ShortSerializer.get());
        serializers.put(Double.class, DoubleSerializer.get());
        serializers.put(double.class, DoubleSerializer.get());
        serializers.put(Float.class, FloatSerializer.get());
        serializers.put(float.class, FloatSerializer.get());
        serializers.put(Boolean.class, BooleanSerializer.get());
        serializers.put(boolean.class, BooleanSerializer.get());
        serializers.put(Date.class, DateSerializer.get());
        serializers.put(byte[].class, BytesArraySerializer.get());
    }

    public static void addSerializer(Class<?> valueClass, AbstractSerializer<?> serializer) {
        serializers.put(valueClass, serializer);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Serializer<T> getSerializer(Object value) {
        Serializer serializer = (value == null) ? ByteBufferSerializer.get() : getSerializer(value.getClass());
        return serializer;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
        Serializer serializer;

        if ((valueClass == null) || (ByteBuffer.class.isAssignableFrom(valueClass))) {
            serializer = ByteBufferSerializer.get();
        } else if (serializers.containsKey(valueClass)) {
            serializer = serializers.get(valueClass);
        } else {
            serializer = ObjectSerializer.get();
        }

        return serializer;
    }
}

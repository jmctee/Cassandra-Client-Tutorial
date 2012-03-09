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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
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
        }
        else if (serializers.containsKey(valueClass)) {
            serializer = serializers.get(valueClass);
        }
        else {
            serializer = ObjectSerializer.get();
        }

        return serializer;
    }
}

package com.jeklsoft.hector.serializer.hector;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.Serializer;

import java.nio.ByteBuffer;

public class ExtendedTypeInferringSerializer<T> extends AbstractSerializer<T> implements Serializer<T> {

    @SuppressWarnings("rawtypes")
    private static final ExtendedTypeInferringSerializer INSTANCE = new ExtendedTypeInferringSerializer();

    @SuppressWarnings("unchecked")
    public static <T> ExtendedTypeInferringSerializer<T> get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(T obj) {
        return ExtensibleTypeInferrringSerializer.getSerializer(obj).toByteBuffer(obj);
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "The type inferring serializer can only be used for data being written to and not data read from the data store");
    }
}

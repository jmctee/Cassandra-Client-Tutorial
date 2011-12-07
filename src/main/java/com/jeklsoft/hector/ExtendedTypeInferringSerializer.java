package com.jeklsoft.hector;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.Serializer;

public class ExtendedTypeInferringSerializer<T> extends AbstractSerializer<T> implements Serializer<T> {

  @SuppressWarnings("rawtypes")
  private static final ExtendedTypeInferringSerializer INSTANCE = new ExtendedTypeInferringSerializer();

  @SuppressWarnings("unchecked")
  public static <T> ExtendedTypeInferringSerializer<T> get() {
    return INSTANCE;
  }

  @Override
  public ByteBuffer toByteBuffer(T obj) {
    return ExtendedSerializerTypeInferer.getSerializer(obj).toByteBuffer(obj);
  }

  @Override
  public T fromByteBuffer(ByteBuffer byteBuffer) {
    throw new IllegalStateException(
        "The type inferring serializer can only be used for data going to the database, and not data coming from the database");
  }
}

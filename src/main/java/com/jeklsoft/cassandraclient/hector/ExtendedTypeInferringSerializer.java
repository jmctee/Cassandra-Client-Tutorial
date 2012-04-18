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
        return ExtensibleTypeInferringSerializer.getSerializer(obj).toByteBuffer(obj);
    }

    @Override
    public T fromByteBuffer(ByteBuffer byteBuffer) {
        throw new IllegalStateException(
                "The type inferring serializer can only be used for data being written to and not data read from the data store");
    }
}

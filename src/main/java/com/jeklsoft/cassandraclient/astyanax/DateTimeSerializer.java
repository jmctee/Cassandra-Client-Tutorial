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

package com.jeklsoft.cassandraclient.astyanax;

import static com.netflix.astyanax.serializers.ComparatorType.LONGTYPE;

import java.nio.ByteBuffer;

import org.joda.time.DateTime;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ComparatorType;

public class DateTimeSerializer extends AbstractSerializer<DateTime> {

    private static final DateTimeSerializer instance = new DateTimeSerializer();

    public static DateTimeSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(DateTime obj) {
        if (obj == null) {
            return null;
        }
        return ByteBuffer.allocate(8).putLong(0, obj.getMillis());
    }

    @Override
    public DateTime fromByteBuffer(ByteBuffer byteBuffer) {
        if ((byteBuffer == null) || (byteBuffer.remaining() < 8)) {
            return null;
        }
        long l = byteBuffer.getLong();
        return new DateTime(l);
    }

    @Override
    public ComparatorType getComparatorType() {
        return LONGTYPE;
    }

}

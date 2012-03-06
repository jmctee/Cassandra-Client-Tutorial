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

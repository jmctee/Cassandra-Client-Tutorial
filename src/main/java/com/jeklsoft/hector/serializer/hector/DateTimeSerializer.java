package com.jeklsoft.hector.serializer.hector;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;

import static me.prettyprint.hector.api.ddl.ComparatorType.LONGTYPE;

public final class DateTimeSerializer extends AbstractSerializer<DateTime> {

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

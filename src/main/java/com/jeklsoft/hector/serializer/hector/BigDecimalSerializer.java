package com.jeklsoft.hector.serializer.hector;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static me.prettyprint.hector.api.ddl.ComparatorType.UTF8TYPE;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

    private static final BigDecimalSerializer instance = new BigDecimalSerializer();
    private static final StringSerializer stringSerializer = StringSerializer.get();

    public static BigDecimalSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(BigDecimal obj) {
        if (obj == null) {
            return null;
        }

        String stringValue = obj.toString();
        return stringSerializer.toByteBuffer(stringValue);
    }

    @Override
    public BigDecimal fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        String stringValue = stringSerializer.fromByteBuffer(byteBuffer);
        return new BigDecimal(stringValue);
    }

    @Override
    public ComparatorType getComparatorType() {
        return UTF8TYPE;
    }
}

package com.jeklsoft.cassandraclient.astyanax;

import static com.netflix.astyanax.serializers.ComparatorType.UTF8TYPE;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ComparatorType;
import com.netflix.astyanax.serializers.StringSerializer;

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

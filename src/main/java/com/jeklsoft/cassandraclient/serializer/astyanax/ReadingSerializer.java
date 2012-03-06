package com.jeklsoft.cassandraclient.serializer.astyanax;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.serializer.ReadingSerializerUtils;
import com.netflix.astyanax.serializers.AbstractSerializer;

public class ReadingSerializer extends AbstractSerializer<Reading> {
    private static final Logger log = Logger.getLogger(ReadingSerializer.class);

    private static final ReadingSerializer instance = new ReadingSerializer();

    public static ReadingSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Reading reading) {
        return ReadingSerializerUtils.toByteBuffer(reading);
    }

    @Override
    public Reading fromByteBuffer(ByteBuffer byteBuffer) {
        return ReadingSerializerUtils.fromByteBuffer(byteBuffer);
    }
}

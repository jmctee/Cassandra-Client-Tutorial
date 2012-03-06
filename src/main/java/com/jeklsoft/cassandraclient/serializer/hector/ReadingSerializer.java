package com.jeklsoft.cassandraclient.serializer.hector;

import static me.prettyprint.hector.api.ddl.ComparatorType.BYTESTYPE;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.serializer.ReadingSerializerUtils;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

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

    @Override
    public ComparatorType getComparatorType() {
        return BYTESTYPE;
    }
}
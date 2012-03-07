package com.jeklsoft.cassandraclient.hector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.ReadingBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.hector.api.Serializer;

public class ReadingSerializer extends AbstractSerializer<Reading> {
    private static final Logger log = Logger.getLogger(ReadingSerializer.class);

    private static final ReadingSerializer instance = new ReadingSerializer();

    public static ReadingSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Reading reading) {
        ReadingBuffer.Reading bufferedReading = getBufferedReading(reading);
        byte[] array = bufferedReading.toByteString().toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(array);
        return buffer;
    }

    @Override
    public Reading fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            int startingIndex = byteBuffer.position();
            int endingIndex = byteBuffer.limit();
            byte[] byteArray = Arrays.copyOfRange(byteBuffer.array(), startingIndex, endingIndex);
            ReadingBuffer.Reading bufferedReading = ReadingBuffer.Reading.newBuilder().mergeFrom(byteArray).build();
            return getReading(bufferedReading);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error deserializing Reading", e);
        }
    }

    private static ReadingBuffer.Reading getBufferedReading(Reading reading) {
        return ReadingBuffer.Reading.newBuilder()
                .setTemperature(getByteString(BigDecimalSerializer.get(), reading.getTemperature()))
                .setWindSpeed(reading.getWindSpeed())
                .setWindDirection(reading.getDirection())
                .setHumidity(getByteString(BigIntegerSerializer.get(), reading.getHumidity()))
                .setBadAirQualityDetected(reading.getBadAirQualityDetected())
                .build();
    }

    private static Reading getReading(ReadingBuffer.Reading bufferedReading) {
        return new Reading((BigDecimal) getObject(BigDecimalSerializer.get(), bufferedReading.getTemperature()),
                bufferedReading.getWindSpeed(),
                bufferedReading.getWindDirection(),
                (BigInteger) getObject(BigIntegerSerializer.get(), bufferedReading.getHumidity()),
                bufferedReading.getBadAirQualityDetected());
    }

    private static ByteString getByteString(Serializer serializer, Object object) {
        byte[] bytes = serializer.toByteBuffer(object).array();
        return ByteString.copyFrom(bytes);
    }

    private static Object getObject(Serializer serializer, ByteString bytes) {
        return serializer.fromBytes(bytes.toByteArray());
    }
}
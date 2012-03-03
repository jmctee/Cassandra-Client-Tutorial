package com.jeklsoft.hector;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Serializer;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class ReadingSerializer extends AbstractSerializer<Reading> {

    private static final ReadingSerializer instance = new ReadingSerializer();

    public static ReadingSerializer get() {
      return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(Reading reading) {
        ReadingBuffer.Reading bufferedReading = getBufferedReading(reading);
        return ByteBuffer.wrap(bufferedReading.toByteString().toByteArray());
    }

    @Override
    public Reading fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            byte[] byteArray = byteBuffer.array();
            ReadingBuffer.Reading bufferedReading = ReadingBuffer.Reading.newBuilder().mergeFrom(byteBuffer.array()).build();
            return getReading(bufferedReading);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error deserializing Reading", e);
        }
    }

    private ReadingBuffer.Reading getBufferedReading(Reading reading) {
        return ReadingBuffer.Reading.newBuilder()
                .setSensorId(getByteString(UUIDSerializer.get(), reading.getSensorId()))
                .setTimestamp(reading.getTimestamp().getMillis())
                .setTemperature(getByteString(BigDecimalSerializer.get(), reading.getTemperature()))
                .setWindSpeed(reading.getWindSpeed())
                .setWindDirection(reading.getDirection())
                .setHumidity(getByteString(BigIntegerSerializer.get(), reading.getHumidity()))
                .setBadAirQualityDetected(reading.getBadAirQualityDetected())
                .build();
    }

    private Reading getReading(ReadingBuffer.Reading bufferedReading) {
        return new Reading((UUID)getObject(UUIDSerializer.get(), bufferedReading.getSensorId()),
                new DateTime(bufferedReading.getTimestamp()),
                (BigDecimal)getObject(BigDecimalSerializer.get(), bufferedReading.getTemperature()),
                bufferedReading.getWindSpeed(),
                bufferedReading.getWindDirection(),
                (BigInteger)getObject(BigIntegerSerializer.get(), bufferedReading.getHumidity()),
                bufferedReading.getBadAirQualityDetected());
    }

    private ByteString getByteString(Serializer serializer, Object object) {
        byte[] bytes = serializer.toByteBuffer(object).array();
        return ByteString.copyFrom(bytes);
    }

    private Object getObject(Serializer serializer, ByteString bytes) {
        return serializer.fromBytes(bytes.toByteArray());
    }

}
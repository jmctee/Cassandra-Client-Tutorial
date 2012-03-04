package com.jeklsoft.hector.serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jeklsoft.hector.Reading;
import com.jeklsoft.hector.ReadingBuffer;
import com.jeklsoft.hector.serializer.hector.BigDecimalSerializer;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Serializer;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class ReadingSerializerUtils {

    public static ByteBuffer toByteBuffer(Reading reading) {
        ReadingBuffer.Reading bufferedReading = getBufferedReading(reading);
        byte[] array = bufferedReading.toByteString().toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(array);
        return buffer;
    }

    public static Reading fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            int startingIndex = byteBuffer.position();
            int endingIndex = byteBuffer.limit();
            byte[] byteArray = Arrays.copyOfRange(byteBuffer.array(), startingIndex, endingIndex);
            ReadingBuffer.Reading bufferedReading = ReadingBuffer.Reading.newBuilder().mergeFrom(byteArray).build();
            return getReading(bufferedReading);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error deserializing Reading", e);
        }
    }

    private static ReadingBuffer.Reading getBufferedReading(Reading reading) {
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

    private static Reading getReading(ReadingBuffer.Reading bufferedReading) {
        return new Reading((UUID) getObject(UUIDSerializer.get(), bufferedReading.getSensorId()),
                new DateTime(bufferedReading.getTimestamp()),
                (BigDecimal) getObject(BigDecimalSerializer.get(), bufferedReading.getTemperature()),
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

package com.jeklsoft.cassandraclient;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.jeklsoft.cassandraclient.ReadingBuffer.Reading;
import com.jeklsoft.cassandraclient.hector.BigDecimalSerializer;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.hector.api.Serializer;

public class TestReadingBuffer {

    private BigDecimal temperature;
    private int windSpeed;
    private String windDirection;
    private BigInteger humidity;
    private Boolean badAirQualityDetected;

    Reading reading;

    @Before
    public void setup() {
        temperature = new BigDecimal(195).movePointLeft(1);
        windSpeed = 27;
        windDirection = "ESE";
        humidity = BigInteger.valueOf(17L);
        badAirQualityDetected = false;

        reading = build();
    }

    @Test
    public void canCreateReadingBuffer() {
        assertEquals(temperature, getObject(BigDecimalSerializer.get(), reading.getTemperature()));
        assertEquals(windSpeed, reading.getWindSpeed());
        assertEquals(windDirection, reading.getWindDirection());
        assertEquals(humidity, getObject(BigIntegerSerializer.get(), reading.getHumidity()));
        assertEquals(badAirQualityDetected, reading.getBadAirQualityDetected());
    }

    @Test
    public void canSerializeAndDeserializeReadingBuffer() throws Exception {

        byte[] array = reading.toByteArray();

        Reading newReading = ReadingBuffer.Reading.newBuilder().mergeFrom(array).build();

        assertEquals(reading, newReading);

    }

    private ReadingBuffer.Reading build() {
        return ReadingBuffer.Reading.newBuilder()
                .setTemperature(getByteString(BigDecimalSerializer.get(), temperature))
                .setWindSpeed(windSpeed)
                .setWindDirection(windDirection)
                .setHumidity(getByteString(BigIntegerSerializer.get(), humidity))
                .setBadAirQualityDetected(badAirQualityDetected)
                .build();
    }

    private ByteString getByteString(Serializer serializer, Object object) {
        byte[] bytes = serializer.toByteBuffer(object).array();
        return ByteString.copyFrom(bytes);
    }

    private Object getObject(Serializer serializer, ByteString bytes) {
        return serializer.fromBytes(bytes.toByteArray());
    }
}

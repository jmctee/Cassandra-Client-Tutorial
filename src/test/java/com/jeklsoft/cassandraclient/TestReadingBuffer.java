//Copyright 2012 Joe McTee
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.jeklsoft.cassandraclient;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.xml.bind.DatatypeConverter;

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

    @Test
    public void canBase64EncodeDecodeReadings() throws Exception {
        byte[] array = reading.toByteArray();
        String string = reading.toString();
        String encoded = DatatypeConverter.printBase64Binary(array);

        byte[] decoded = DatatypeConverter.parseBase64Binary(encoded);
        Reading newReading = ReadingBuffer.Reading.parseFrom(decoded);
        String newString = newReading.toString();

        assertEquals(reading, newReading);
        assertEquals(string, newString);
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

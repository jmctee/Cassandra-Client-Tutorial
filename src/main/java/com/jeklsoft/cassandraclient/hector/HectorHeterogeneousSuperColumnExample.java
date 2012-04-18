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

package com.jeklsoft.cassandraclient.hector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.ReadingsPersistor;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperSliceQuery;

/*
Cluster: SensorNet

Keyspace: "Climate" {
    CF<supercolumn>: "Sensors" {
        sensor_uuid: {
            <timestamp: DateTime> : {
                "Temperature" : <BigDecimal>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }

            ...

            <timestamp: DateTime> : {
                "Temperature" : <BigDecimal>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }
        }

        ...

        sensor_uuid: {
            <timestamp: DateTime> : {
                "Temperature" : <BigDecimal>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }

            ...

            <timestamp: DateTime> : {
                "Temperature" : <BigDecimal>
                "WindSpeed" : <Integer>
                "WindDirection" : <String>
                "Humidity" : <BigInteger>
                "BadAirQualityDetected" : <Boolean>
            }
        }
    }
}
*/

public class HectorHeterogeneousSuperColumnExample implements ReadingsPersistor {

    private static final Logger log = Logger.getLogger(HectorHeterogeneousSuperColumnExample.class);

    private static final Serializer genericOutputSerializer = ExtendedTypeInferringSerializer.get();
    private static final Serializer ss = StringSerializer.get();
    private static final Serializer us = UUIDSerializer.get();
    private static final Serializer ls = LongSerializer.get();

    private static final String temperatureNameColumnName = "Temperature";
    private static final String windSpeedNameColumnName = "WindSpeed";
    private static final String windDirectionNameColumnName = "WindDirection";
    private static final String humidityNameColumnName = "Humidity";
    private static final String badAirQualityDetectedNameColumnName = "BadAirQualityDetected";

    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final int ttl = 365 * 24 * 60 * 60; // 1 year in seconds

    public HectorHeterogeneousSuperColumnExample(Keyspace keyspace, String columnFamilyName) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;

        ExtensibleTypeInferringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        ExtensibleTypeInferringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        ExtensibleTypeInferringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
    }

    @Override
    public void addReadings(final List<Reading> readings) {
        Mutator mutator = HFactory.createMutator(keyspace, genericOutputSerializer);

        for (Reading reading : readings) {
            HColumn temperatureColumn = HFactory.createColumn(temperatureNameColumnName,
                    reading.getTemperature(),
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);
            HColumn windSpeedColumn = HFactory.createColumn(windSpeedNameColumnName,
                    reading.getWindSpeed(),
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);
            HColumn windDirectionColumn = HFactory.createColumn(windDirectionNameColumnName,
                    reading.getDirection(),
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);
            HColumn humidityColumn = HFactory.createColumn(humidityNameColumnName,
                    reading.getHumidity(),
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);
            HColumn badAirQualityDetectedColumn = HFactory.createColumn(badAirQualityDetectedNameColumnName,
                    reading.getBadAirQualityDetected(),
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);

            List columnList = new ArrayList();
            columnList.add(temperatureColumn);
            columnList.add(windSpeedColumn);
            columnList.add(windDirectionColumn);
            columnList.add(humidityColumn);
            columnList.add(badAirQualityDetectedColumn);

            HSuperColumn superColumn = HFactory.createSuperColumn(reading.getTimestamp(),
                    columnList,
                    genericOutputSerializer,
                    genericOutputSerializer,
                    genericOutputSerializer);

            mutator.addInsertion(reading.getSensorId(), columnFamilyName, superColumn);
        }

        mutator.execute();
    }

    @Override
    public List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn) {
        SuperSliceQuery query = HFactory.createSuperSliceQuery(keyspace, us, ls, ss, ByteBufferSerializer.get());

        query.setColumnFamily(columnFamilyName).setKey(sensorId).setRange(interval.getStartMillis(), interval.getEndMillis(),
                false, maxToReturn);

        QueryResult<SuperSlice<UUID, String, ByteBuffer>> result = query.execute();

        List<HSuperColumn<UUID, String, ByteBuffer>> rows = result.get().getSuperColumns();

        List<Reading> readings = new ArrayList<Reading>();

        for (HSuperColumn row : rows) {
            Reading reading = getReadingFromSuperColumn(sensorId, row);
            readings.add(reading);
        }
        return readings;
    }

    private Reading getReadingFromSuperColumn(UUID sensorId, HSuperColumn row) {
        DateTime timestamp = new DateTime(row.getName());
        BigDecimal temperature = null;
        Integer windSpeed = null;
        String windDirection = null;
        BigInteger humidity = null;
        Boolean badAirQualityDetected = null;

        List<HColumn<String, ByteBuffer>> columns = row.getColumns();
        for (HColumn<String, ByteBuffer> column : columns) {
            if (temperatureNameColumnName.equals(column.getName())) {
                temperature = BigDecimalSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (windSpeedNameColumnName.equals(column.getName())) {
                windSpeed = IntegerSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (windDirectionNameColumnName.equals(column.getName())) {
                windDirection = StringSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (humidityNameColumnName.equals(column.getName())) {
                humidity = BigIntegerSerializer.get().fromByteBuffer(column.getValue());
            }
            else if (badAirQualityDetectedNameColumnName.equals(column.getName())) {
                badAirQualityDetected = BooleanSerializer.get().fromByteBuffer(column.getValue());
            }
            else {
                throw new RuntimeException("Unknown column name " + column.getName());
            }
        }

        Validate.notNull(temperature, "Temperature not found in retrieved super column");
        Validate.notNull(windSpeed, "Wind speed not found in retrieved super column");
        Validate.notNull(windDirection, "Wind Direction not found in retrieved super column");
        Validate.notNull(humidity, "Humidity not found in retrieved super column");
        Validate.notNull(badAirQualityDetected, "Bad air quality detection not found in retrieved super column");

        Reading reading = new Reading(sensorId, timestamp, temperature, windSpeed, windDirection, humidity, badAirQualityDetected);
        return reading;
    }
}

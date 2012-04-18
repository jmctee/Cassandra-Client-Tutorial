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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.ReadingsPersistor;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/*
Cluster: SensorNet

Keyspace: "Climate" {
    CF: "Sensors" {
        sensor_uuid: {
            <timestamp: DateTime> : reading<ReadingBuffer>
            ...
            <timestamp: DateTime> : reading<ReadingBuffer>
        }
        ...
        sensor_uuid: {
            <timestamp: DateTime> : reading<ReadingBuffer>
            ...
            <timestamp: DateTime> : reading<ReadingBuffer>
        }
    }
}
*/

public class HectorProtocolBufferWithStandardColumnExample implements ReadingsPersistor {
    private static final Logger log = Logger.getLogger(HectorProtocolBufferWithStandardColumnExample.class);

    private static final Serializer genericOutputSerializer = ExtendedTypeInferringSerializer.get();
    private static final Serializer us = UUIDSerializer.get();
    private static final Serializer ds = DateTimeSerializer.get();
    private static final Serializer rs = ReadingSerializer.get();

    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final int ttl = 365 * 24 * 60 * 60; // 1 year in seconds

    public HectorProtocolBufferWithStandardColumnExample(Keyspace keyspace, String columnFamilyName) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;

        ExtensibleTypeInferringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        ExtensibleTypeInferringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        ExtensibleTypeInferringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
        ExtensibleTypeInferringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
    }

    @Override
    public void addReadings(final List<Reading> readings) {
        Mutator mutator = HFactory.createMutator(keyspace, genericOutputSerializer);

        for (Reading reading : readings) {
            HColumn column = HFactory.createColumn(reading.getTimestamp(),
                    reading,
                    genericOutputSerializer,
                    genericOutputSerializer).setTtl(ttl);

            mutator.addInsertion(reading.getSensorId(), columnFamilyName, column);
        }

        mutator.execute();
    }

    @Override
    public List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn) {
        SliceQuery<UUID, DateTime, Reading> query = HFactory.createSliceQuery(keyspace, us, ds, rs);

        query.setColumnFamily(columnFamilyName)
                .setKey(sensorId)
                .setRange(new DateTime(interval.getStartMillis()), new DateTime(interval.getEndMillis()), false, maxToReturn);

        QueryResult<ColumnSlice<DateTime, Reading>> result = query.execute();

        List<HColumn<DateTime, Reading>> columns = result.get().getColumns();

        List<Reading> readings = new ArrayList<Reading>();

        for (HColumn column : columns) {
            DateTime timestamp = (DateTime) column.getName();
            Reading reading = new Reading(sensorId, timestamp, (Reading) column.getValue());
            readings.add(reading);
        }

        return readings;
    }
}

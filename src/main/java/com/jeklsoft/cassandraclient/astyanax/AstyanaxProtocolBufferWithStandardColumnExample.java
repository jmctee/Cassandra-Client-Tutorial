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

package com.jeklsoft.cassandraclient.astyanax;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.jeklsoft.cassandraclient.Reading;
import com.jeklsoft.cassandraclient.ReadingsPersistor;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.util.RangeBuilder;

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

public class AstyanaxProtocolBufferWithStandardColumnExample implements ReadingsPersistor {
    private static final Logger log = Logger.getLogger(AstyanaxProtocolBufferWithStandardColumnExample.class);

    private final Keyspace keyspace;
    private final ColumnFamily<UUID, DateTime> columnFamilyInfo;
    private final int ttl = 365 * 24 * 60 * 60; // 1 year in seconds

    public AstyanaxProtocolBufferWithStandardColumnExample(Keyspace keyspace, ColumnFamily<UUID, DateTime> columnFamilyInfo) {
        this.keyspace = keyspace;
        this.columnFamilyInfo = columnFamilyInfo;
    }

    @Override
    public void addReadings(List<Reading> readings) {
        MutationBatch m = keyspace.prepareMutationBatch();

        for (Reading reading : readings) {
            m.withRow(columnFamilyInfo, reading.getSensorId())
                    .putColumn(reading.getTimestamp(), reading, ReadingSerializer.get(), ttl);
        }

        try {
            // note: you still have access to m.execute() if you don't need async feature.
            Future<OperationResult<Void>> future = m.executeAsync();

            // do some other stuff here...

            OperationResult<Void> result = future.get();
        }
        catch (ConnectionException e) {
            throw new RuntimeException("Storage of readings failed", e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Storage of readings failed", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Storage of readings failed", e);
        }
    }

    @Override
    public List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn) {

        List<Reading> readings = new ArrayList<Reading>();

        ByteBufferRange range = new RangeBuilder().setLimit(maxToReturn)
                .setStart(interval.getStartMillis())
                .setEnd(interval.getEndMillis())
                .build();

        RowQuery<UUID, DateTime> query = keyspace.prepareQuery(columnFamilyInfo)
                .getKey(sensorId)
                .autoPaginate(true)
                .withColumnRange(range);

        try {
            // Again query.execute() is available here is synchronous behavior desired.

            Future<OperationResult<ColumnList<DateTime>>> future = query.executeAsync();

            // time passes...

            OperationResult<ColumnList<DateTime>> result = future.get();

            ColumnList columns = result.getResult();

            Iterator ii = columns.iterator();
            while (ii.hasNext()) {
                Column column = (Column) ii.next();
                DateTime timestamp = (DateTime) column.getName();
                Reading reading = new Reading(sensorId, timestamp, (Reading) column.getValue(ReadingSerializer.get()));
                readings.add(reading);
            }
        }
        catch (ConnectionException e) {
            throw new RuntimeException("Query failed", e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Query failed", e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Query failed", e);
        }

        return readings;
    }
}

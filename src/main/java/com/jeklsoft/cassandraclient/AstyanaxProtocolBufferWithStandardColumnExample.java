package com.jeklsoft.cassandraclient;

import com.jeklsoft.cassandraclient.serializer.astyanax.ReadingSerializer;
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
import org.apache.log4j.Logger;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

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
    private static final Logger log = Logger.getLogger(HectorProtocolBufferWithStandardColumnExample.class);

    private final Keyspace keyspace;
    private final ColumnFamily<UUID, Long> columnFamilyInfo;
    private final int ttl = 365 * 24 * 60 * 60; // 1 year in seconds

    public AstyanaxProtocolBufferWithStandardColumnExample(Keyspace keyspace, ColumnFamily<UUID, Long> columnFamilyInfo) {
        this.keyspace = keyspace;
        this.columnFamilyInfo = columnFamilyInfo;
    }

    @Override
    public void addReadings(List<Reading> readings) {
        MutationBatch m = keyspace.prepareMutationBatch();

        for (Reading reading : readings) {
            m.withRow(columnFamilyInfo, reading.getSensorId())
                    .putColumn(reading.getTimestamp().getMillis(), reading, ReadingSerializer.get(), ttl);
        }

        try {
            OperationResult<Void> result = m.execute();
        } catch (ConnectionException e) {
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

        RowQuery<UUID, Long> query = keyspace.prepareQuery(columnFamilyInfo)
                .getKey(sensorId)
                .autoPaginate(true)
                .withColumnRange(range);

        try {
            OperationResult result = query.execute();
            ColumnList columns = (ColumnList) result.getResult();

            Iterator ii = columns.iterator();
            while (ii.hasNext()) {
                Column column = (Column) ii.next();
                Reading reading = (Reading) column.getValue(ReadingSerializer.get());
                readings.add(reading);
            }
        } catch (ConnectionException e) {
            throw new RuntimeException("Query failed", e);
        }

        return readings;
    }
}

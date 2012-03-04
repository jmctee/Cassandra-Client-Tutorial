package com.jeklsoft.cassandraclient;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import org.apache.log4j.Logger;
import org.joda.time.Interval;

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

    public AstyanaxProtocolBufferWithStandardColumnExample(Keyspace keyspace, ColumnFamily<UUID, Long> columnFamilyInfo) {
        this.keyspace = keyspace;
        this.columnFamilyInfo = columnFamilyInfo;
    }

    @Override
    public void addReadings(List<Reading> readings) {
//        MutationBatch m = keyspace.prepareMutationBatch();
//
//        for (Reading reading : readings) {
//            m.withRow(columnFamilyInfo, reading.getSensorId()).putColumn(reading.getTimestamp().getMillis(), reading, ReadingSerializer.get(), 1);
//        }
//
//        try {
//            OperationResult<Void> result = m.execute();
//        }
//        catch (ConnectionException e) {
//            throw new RuntimeException("Storage of readings failed", e);
//        }
    }

    @Override
    public List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

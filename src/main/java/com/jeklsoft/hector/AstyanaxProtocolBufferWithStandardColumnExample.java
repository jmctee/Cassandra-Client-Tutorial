package com.jeklsoft.hector;

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

    public AstyanaxProtocolBufferWithStandardColumnExample() {

    }

    @Override
    public void addReadings(List<Reading> readings) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

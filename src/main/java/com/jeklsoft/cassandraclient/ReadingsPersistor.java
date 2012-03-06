package com.jeklsoft.cassandraclient;

import java.util.List;
import java.util.UUID;

import org.joda.time.Interval;

public interface ReadingsPersistor {
    void addReadings(final List<Reading> readings);

    List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn);
}

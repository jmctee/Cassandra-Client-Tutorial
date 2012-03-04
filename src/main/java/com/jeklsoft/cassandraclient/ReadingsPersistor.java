package com.jeklsoft.cassandraclient;

import org.joda.time.Interval;

import java.util.List;
import java.util.UUID;

public interface ReadingsPersistor {
    void addReadings(final List<Reading> readings);

    List<Reading> querySensorReadingsByInterval(UUID sensorId, Interval interval, int maxToReturn);
}

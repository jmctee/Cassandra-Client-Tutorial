package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Test;

// TODO: Need mechanism to auto copy cassandra.yaml to /tmp/cassandra

public class TestHectorHeterogeneousSuperClassExample {

    @Test
    public void testHectorAccess() throws Exception {
        HectorHeterogeneousSuperClassExample example = new HectorHeterogeneousSuperClassExample();

        UUID sensorId1 = new UUID(0,100);
        UUID sensorId2 = new UUID(0,200);

        DateTime baseDate = new DateTime();
        int readingInterval = 15; // 15 minutes

        List<Reading> readings = new ArrayList<Reading>();

        for (int ii=0; ii<10; ii++) {

            BigDecimal temperature = new BigDecimal(230 + ii).movePointLeft(1);
            readings.add(new Reading(sensorId1, baseDate.plusMinutes(readingInterval * ii), temperature,
                         16, "W", BigInteger.valueOf(17L), false));

            temperature = new BigDecimal(195 - ii).movePointLeft(1);
            readings.add(new Reading(sensorId2, baseDate.plusMinutes(readingInterval * ii), temperature,
                         24, "ESE", BigInteger.valueOf(17L), false));
        }

        List<Reading> expectedReadings = new ArrayList<Reading>();
        for (int ii=0; ii<3; ii++) {
            BigDecimal temperature = new BigDecimal(234 + ii).movePointLeft(1);
            expectedReadings.add(new Reading(sensorId1, baseDate.plusMinutes(readingInterval * (ii + 4)),
                                             temperature, 16, "W", BigInteger.valueOf(17L), false));
        }

        example.addReadings(readings);

        DateTime startTime = new DateTime(baseDate.plusMinutes(readingInterval * 4));
        DateTime endTime = new DateTime(baseDate.plusMinutes(readingInterval * 6));
        Duration duration = new Duration(startTime, endTime);
        Interval interval = new Interval(startTime, duration);

        List<Reading> returnedReadings = example.querySensorReadingsByTime(sensorId1, interval, 10);

        assertEquals(expectedReadings.size(), returnedReadings.size());

        for (Reading expectedReading : expectedReadings)
        {
            assertTrue(returnedReadings.contains(expectedReading));
        }
    }
}

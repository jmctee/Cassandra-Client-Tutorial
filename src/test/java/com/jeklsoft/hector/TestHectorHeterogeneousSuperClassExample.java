package com.jeklsoft.hector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class TestHectorHeterogeneousSuperClassExample {

    @Test
    public void testHectorAccess() throws Exception {
        HectorHeterogeneousSuperClassExample example = new HectorHeterogeneousSuperClassExample();

        UUID sensorId1 = new UUID(0,100);
        UUID sensorId2 = new UUID(0,200);

        Date baseDate = new Date();
        int readingInterval = 15 * 60 * 1000; // 15 minutes

        List<Reading> readings = new ArrayList<Reading>();

        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 0)), 23.0, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 1)), 23.1, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 2)), 23.2, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 3)), 23.3, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 4)), 23.4, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 5)), 23.5, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 6)), 23.6, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 7)), 23.7, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 8)), 23.8, 16, "W", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 9)), 23.9, 16, "W", BigInteger.valueOf(17L), false));

        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 0)), 19.5, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 1)), 19.4, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 2)), 19.3, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 3)), 19.2, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 4)), 19.1, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 5)), 19.0, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 6)), 18.9, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 7)), 18.8, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 8)), 18.7, 24, "ESE", BigInteger.valueOf(17L), false));
        readings.add(new Reading(sensorId2, new Date(baseDate.getTime() + (readingInterval * 9)), 18.6, 24, "ESE", BigInteger.valueOf(17L), false));

        List<Reading> expectedReadings = new ArrayList<Reading>();
        expectedReadings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 4)), 23.4, 16, "W", BigInteger.valueOf(17L), false));
        expectedReadings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 5)), 23.5, 16, "W", BigInteger.valueOf(17L), false));
        expectedReadings.add(new Reading(sensorId1, new Date(baseDate.getTime() + (readingInterval * 6)), 23.6, 16, "W", BigInteger.valueOf(17L), false));

        example.addReadings(readings);

        List<Reading> returnedReadings = example.querySensorReadingsByTime(sensorId1, new Date(baseDate.getTime() + (readingInterval * 4)), new Date(baseDate.getTime() + (readingInterval * 6)), 10);

        assertEquals(expectedReadings.size(), returnedReadings.size());

        for (Reading expectedReading : expectedReadings)
        {
            assertTrue(returnedReadings.contains(expectedReading));
        }
    }
}

package com.jeklsoft.hector;

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
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

public class HectorProtocolBufferWithStandardColumnExample implements ReadingsPersistor {
    private static final Logger log = Logger.getLogger(HectorProtocolBufferWithStandardColumnExample.class);

    private static final Serializer genericOutputSerializer = ExtendedTypeInferringSerializer.get();
    private static final Serializer us = UUIDSerializer.get();
    private static final Serializer ds = DateTimeSerializer.get();
    private static final Serializer rs = ReadingSerializer.get();

    private final Keyspace keyspace;
    private final String columnFamilyName;

    public HectorProtocolBufferWithStandardColumnExample(Keyspace keyspace, String columnFamilyName) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;

        ExtensibleTypeInferrringSerializer.addSerializer(BigInteger.class, BigIntegerSerializer.get());
        ExtensibleTypeInferrringSerializer.addSerializer(DateTime.class, DateTimeSerializer.get());
        ExtensibleTypeInferrringSerializer.addSerializer(BigDecimal.class, BigDecimalSerializer.get());
        ExtensibleTypeInferrringSerializer.addSerializer(Reading.class, ReadingSerializer.get());
    }

    @Override
    public void addReadings(final List<Reading> readings) {
        Mutator mutator = HFactory.createMutator(keyspace, genericOutputSerializer);

        for (Reading reading : readings) {
            HColumn column = HFactory.createColumn(reading.getTimestamp(),
                    reading,
                    genericOutputSerializer,
                    genericOutputSerializer);

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

        List<ByteBuffer> buffers = getArrays(columns.get(0).getValueBytes(), columns.size());
        List<Reading> readings = new ArrayList<Reading>();

        for (ByteBuffer buffer : buffers) {
            Reading reading = ReadingSerializer.get().fromByteBuffer(buffer);
            readings.add(reading);
        }
//        for (HColumn column : columns)
//        {
//            Reading reading = (Reading)column.getValue();
//            readings.add(reading);
//        }

        return readings;
    }

    private List<ByteBuffer> getArrays(ByteBuffer buffer, int expectedCount) {
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

        byte[] bytes = buffer.array();
        for (int ii = 0; ii < expectedCount; ii++) {

            int start = 54 + (79 * ii);
            int end = start + 41;

            byte[] array = ArrayUtils.subarray(bytes, start, end);
            buffers.add(ByteBuffer.wrap(array));
        }

        return buffers;
    }

//    private void dumpArray(String prefix, byte[] array) {
//
//        System.out.print(prefix);
//        String line = "";
//        for (byte value : array) {
//            line += String.format("%02x", value);
//        }
//        System.out.println(line);
//    }
}

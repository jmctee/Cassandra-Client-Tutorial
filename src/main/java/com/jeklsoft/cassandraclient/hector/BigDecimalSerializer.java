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

import static me.prettyprint.hector.api.ddl.ComparatorType.UTF8TYPE;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class BigDecimalSerializer extends AbstractSerializer<BigDecimal> {

    private static final BigDecimalSerializer instance = new BigDecimalSerializer();
    private static final StringSerializer stringSerializer = StringSerializer.get();

    public static BigDecimalSerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(BigDecimal obj) {
        if (obj == null) {
            return null;
        }

        String stringValue = obj.toString();
        return stringSerializer.toByteBuffer(stringValue);
    }

    @Override
    public BigDecimal fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        String stringValue = stringSerializer.fromByteBuffer(byteBuffer);
        return new BigDecimal(stringValue);
    }

    @Override
    public ComparatorType getComparatorType() {
        return UTF8TYPE;
    }
}

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

package com.jeklsoft.cassandraclient;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.DateTime;

public class Reading {

    private final UUID sensorId;
    private final DateTime timestamp;
    private final BigDecimal temperature;
    private final Integer windSpeed;
    private final String direction;
    private final BigInteger humidity;
    private final Boolean badAirQualityDetected;

    public Reading(UUID sensorId, DateTime timestamp, BigDecimal temperature, Integer windSpeed, String direction,
                   BigInteger humidity, Boolean badAirQualityDetected) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.direction = direction;
        this.humidity = humidity;
        this.badAirQualityDetected = badAirQualityDetected;
    }

    public Reading(BigDecimal temperature, Integer windSpeed, String direction,
                   BigInteger humidity, Boolean badAirQualityDetected) {
        this.sensorId = null;
        this.timestamp = null;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.direction = direction;
        this.humidity = humidity;
        this.badAirQualityDetected = badAirQualityDetected;
    }

    public Reading(UUID sensorId, DateTime timestamp, Reading reading) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.temperature = reading.getTemperature();
        this.windSpeed = reading.getWindSpeed();
        this.direction = reading.getDirection();
        this.humidity = reading.getHumidity();
        this.badAirQualityDetected = reading.getBadAirQualityDetected();
    }

    public UUID getSensorId() {
        return sensorId;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public BigDecimal getTemperature() {
        return temperature;
    }

    public Integer getWindSpeed() {
        return windSpeed;
    }

    public String getDirection() {
        return direction;
    }

    public BigInteger getHumidity() {
        return humidity;
    }

    public Boolean getBadAirQualityDetected() {
        return badAirQualityDetected;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

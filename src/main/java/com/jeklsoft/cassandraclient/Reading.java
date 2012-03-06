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

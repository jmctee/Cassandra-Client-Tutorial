package com.jeklsoft.hector;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

public class Reading {

    private final UUID sensorId;
    private final Date timestamp;
    private final Double temperature;
    private final Integer windSpeed;
    private final String direction;
    private final BigInteger humidity;
    private final Boolean badAirQualityDetected;

    public Reading(UUID sensorId, Date timestamp, Double temperature, Integer windSpeed, String direction,
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

    public Date getTimestamp() {
        return timestamp;
    }

    public Double getTemperature() {
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

package com.jeklsoft.hector;

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

    public Reading(UUID sensorId, Date timestamp, Double temperature, Integer windSpeed, String direction, BigInteger humidity, Boolean badAirQualityDetected) {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Reading reading = (Reading) o;

        if (badAirQualityDetected != null ? !badAirQualityDetected.equals(reading.badAirQualityDetected) : reading.badAirQualityDetected != null)
            return false;
        if (direction != null ? !direction.equals(reading.direction) : reading.direction != null) return false;
        if (humidity != null ? !humidity.equals(reading.humidity) : reading.humidity != null) return false;
        if (sensorId != null ? !sensorId.equals(reading.sensorId) : reading.sensorId != null) return false;
        if (temperature != null ? !temperature.equals(reading.temperature) : reading.temperature != null) return false;
        if (timestamp != null ? !timestamp.equals(reading.timestamp) : reading.timestamp != null) return false;
        if (windSpeed != null ? !windSpeed.equals(reading.windSpeed) : reading.windSpeed != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sensorId != null ? sensorId.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        result = 31 * result + (windSpeed != null ? windSpeed.hashCode() : 0);
        result = 31 * result + (direction != null ? direction.hashCode() : 0);
        result = 31 * result + (humidity != null ? humidity.hashCode() : 0);
        result = 31 * result + (badAirQualityDetected != null ? badAirQualityDetected.hashCode() : 0);
        return result;
    }
}

package com.gamma.skybase.build.server.etl.utils;

public class ReferenceDimAirService {
    private String airServiceNode, eventType, airServiceHost, serviceIdKey, populationDateTime, updateDateTime;

    public String getAirServiceNode() {
        return airServiceNode;
    }

    public void setAirServiceNode(String airServiceNode) {
        this.airServiceNode = airServiceNode;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getAirServiceHost() {
        return airServiceHost;
    }

    public void setAirServiceHost(String airServiceHost) {
        this.airServiceHost = airServiceHost;
    }

    public String getServiceIdKey() {
        return serviceIdKey;
    }

    public void setServiceIdKey(String serviceIdKey) {
        this.serviceIdKey = serviceIdKey;
    }

    public String getPopulationDateTime() {
        return populationDateTime;
    }

    public void setPopulationDateTime(String populationDateTime) {
        this.populationDateTime = populationDateTime;
    }

    public String getUpdateDateTime() {
        return updateDateTime;
    }

    public void setUpdateDateTime(String updateDateTime) {
        this.updateDateTime = updateDateTime;
    }
}

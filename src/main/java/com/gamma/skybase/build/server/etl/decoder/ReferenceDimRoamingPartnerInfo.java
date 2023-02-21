package com.gamma.skybase.build.server.etl.decoder;

public class ReferenceDimRoamingPartnerInfo {
    String mcc, mnc, tadigCode, cc, operatorCountry, operatorName, populationDateTime, updateDateTime;

    public String getMcc() {
        return mcc;
    }

    public void setMcc(String mcc) {
        this.mcc = mcc;
    }

    public String getMnc() {
        return mnc;
    }

    public void setMnc(String mnc) {
        this.mnc = mnc;
    }

    public String getTadigCode() {
        return tadigCode;
    }

    public void setTadigCode(String tadigCode) {
        this.tadigCode = tadigCode;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        this.cc = cc;
    }

    public String getOperatorCountry() {
        return operatorCountry;
    }

    public void setOperatorCountry(String operatorCountry) {
        this.operatorCountry = operatorCountry;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
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

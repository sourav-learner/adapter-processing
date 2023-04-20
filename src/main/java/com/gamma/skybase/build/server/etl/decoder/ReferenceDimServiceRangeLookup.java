package com.gamma.skybase.build.server.etl.decoder;

import com.gamma.components.commons.DateUtility;

import java.text.ParseException;
import java.util.Date;

public class ReferenceDimServiceRangeLookup {

    private String id, lookupKey, lookupOutput;
    private String  validFrom, validTo, populationDateTime, updateDateTime;
    private Date validFromUtilJavaDate, validToFromUtilJavaDate,
            populationDateTimeFromUtilJavaDate, updateDateTimeFromUtilJavaDate;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLookupKey() {
        return lookupKey;
    }

    public void setLookupKey(String lookupKey) {
        this.lookupKey = lookupKey;
    }

    public String getLookupOutput() {
        return lookupOutput;
    }

    public void setLookupOutput(String lookupOutput) {
        this.lookupOutput = lookupOutput;
    }

    public String getValidFrom() {
        return validFrom;
    }

    public void setValidFrom(String validFrom) {
        this.validFrom = validFrom;
        if (validFrom != null && !validFrom.isEmpty()) {
            try {
                this.validFromUtilJavaDate = DateUtility.convertString2JavaUtilDate( validFrom, "dd/MM/yy");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getValidTo() {
        return validTo;
    }

    public void setValidTo(String validTo) {
        this.validTo = validTo;
        if (validTo != null && !validTo.isEmpty()) {
            try {
                this.validToFromUtilJavaDate = DateUtility.convertString2JavaUtilDate(validTo, "dd/MM/yy");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getPopulationDateTime() {
        return populationDateTime;
    }

    public void setPopulationDateTime(String populationDateTime) {
        this.populationDateTime = populationDateTime;
        if (populationDateTime != null && !populationDateTime.isEmpty()) {
            try {
                this.populationDateTimeFromUtilJavaDate = DateUtility.convertString2JavaUtilDate( populationDateTime, "dd/MM/yy");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getUpdateDateTime() {
        return updateDateTime;
    }

    public void setUpdateDateTime(String updateDateTime) {
        this.updateDateTime = updateDateTime;
        if (updateDateTime != null && !updateDateTime.isEmpty()) {
            try {
                this.updateDateTimeFromUtilJavaDate = DateUtility.convertString2JavaUtilDate(updateDateTime, "dd/MM/yy");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Date getValidFromUtilJavaDate() {
        return validFromUtilJavaDate;
    }

    public Date getValidToFromUtilJavaDate() {
        return validToFromUtilJavaDate;
    }

    public Date getPopulationDateTimeFromUtilJavaDate() {
        return populationDateTimeFromUtilJavaDate;
    }

    public Date getUpdateDateTimeFromUtilJavaDate() {
        return updateDateTimeFromUtilJavaDate;
    }
}

package com.gamma.skybase.build.server.etl.decoder.cbs_subscription_cm;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsSubscriptionCmEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private CbsSubscriptionCmEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsSubscriptionCmEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsSubscriptionCmEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String status;

    public Optional<String> getStatus() {
        status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "A":
                    status = "VALID";
                    break;
                case "D":
                    status = "INVALID";
                    break;
                default:
                    status = "UNKNOWN";
                    break;
            }
        }
        if (status != null)
            return Optional.of(status);
        return Optional.empty();
    }

    Date callStartTime;
    String genFullDate;

    public Optional<String> getStartTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callStartTime = sdfS.get().parse(s);
                genFullDate = fullDate.get().format(callStartTime);
                return Optional.of(sdfT.get().format(callStartTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    Date callEndTime;

    public Optional<String> getEndTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callEndTime = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(callEndTime));
            }
        }
        catch (Exception e){
        }
        return Optional.empty();
    }

    String objType;

    public Optional<String> getObjType() {
        objType = getValue("OBJ_TYPE");
        if (objType != null) {
            switch (objType) {
                case "A":
                    objType = "ACC";
                    break;
                case "C":
                    objType = "CUSTOMER";
                    break;
                case "S":
                    objType = "SUBSCRIBER";
                    break;
                case "G":
                    objType = "GROUP";
                    break;
                default:
                    objType = "UNKNOWN";
                    break;
            }
        }
        if (objType != null)
            return Optional.of(objType);
        return Optional.empty();
    }

    String resultCode;

    public Optional<String> getResultCode() {
        resultCode = getValue("RESULT_CODE");
        if (resultCode != null) {
            switch (resultCode) {
                case "0":
                    resultCode = "Success";
                    break;
                default:
                    resultCode = "Fail";
                    break;
            }
        }
        if (resultCode != null)
            return Optional.of(resultCode);
        return Optional.empty();
    }
}
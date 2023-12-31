package com.gamma.skybase.build.server.etl.decoder.cbs_transfer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsTransferEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private CbsTransferEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsTransferEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsTransferEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    Long callDuration;

    public Optional<String> getCallDuration(String field) {
        String s = getValue(field);
        callDuration = 0L;
        try {
            if (s != null)
                callDuration = Long.parseLong(s);
            return Optional.of(String.valueOf(callDuration));
        } catch (NumberFormatException e) { // Ignore non numbers
        }
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

    public Optional<String> getEndTime(String time, String callEventDuration) {
        if (callDuration == null) getCallDuration(callEventDuration);
        if (callStartTime == null) getStartTime(time);

        if (callStartTime == null || callDuration == null) return Optional.empty();

        callEndTime = new Date(callStartTime.getTime() / 1000 + callDuration);
        return Optional.of(sdfT.get().format(callEndTime));
    }

    String resultCode;

    public Optional<String> getResultCode() {
        resultCode = getValue("RESULT_CODE");
        if (resultCode != null) {
            switch (resultCode) {
                case "0":
                    resultCode = "SUCCESSFUL";
                    break;
                default:
                    resultCode = "UNSUCCESSFUL";
                    break;
            }
        }
        if (resultCode != null)
            return Optional.of(resultCode);
        return Optional.empty();
    }

    String transferType;

    public Optional<String> getTransferType() {
        transferType = getValue("TRANSFER_TYPE");
        if (transferType != null) {
            switch (transferType) {
                case "0":
                    transferType = "TRNSFER IN";
                    break;
                case "1":
                    transferType = "TRANSFER OUT";
                    break;
                default:
                    transferType = "-99";
                    break;
            }
        }
        if (transferType != null)
            return Optional.of(transferType);
        return Optional.empty();
    }

    String status;

    public Optional<String> getStatus() {
        status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "F":
                    status = "FINISHED";
                    break;
                case "R":
                    status = "REVERSE";
                    break;
                default:
                    status = "-99";
                    break;
            }
        }
        if (status != null)
            return Optional.of(status);
        return Optional.empty();
    }
}
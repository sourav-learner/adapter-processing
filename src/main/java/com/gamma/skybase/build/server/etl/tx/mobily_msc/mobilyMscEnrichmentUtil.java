package com.gamma.skybase.build.server.etl.tx.mobily_msc;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class mobilyMscEnrichmentUtil {

    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private mobilyMscEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static mobilyMscEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new mobilyMscEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String callIndicator, servedMSISDN;
    String A_Number, B_Number;

    public Optional<String> getServedMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        A_Number = getValue("A_NUMBER");
        B_Number = getValue("B_NUMBER");
        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                    if (A_Number != null) {
                        servedMSISDN = A_Number;
                    }
                    break;

                case "mtc":
                case "fwd":
                    if (B_Number != null) {
                        servedMSISDN = B_Number;
                    }
                    break;
            }

        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String thirdPartyMSISDN;

    public Optional<String> getThirdPartyMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        A_Number = getValue("A_NUMBER");
        if (callIndicator != null)
            if (callIndicator.toLowerCase().equals("fwd")) {
                if (A_Number != null) {
                    thirdPartyMSISDN = A_Number;
                } else {
                    thirdPartyMSISDN = "-99";
                }
            }

        if (thirdPartyMSISDN != null)
            return Optional.of(thirdPartyMSISDN);
        return Optional.empty();
    }

    String otherMSISDN, C_Number;

    public Optional<String> getOtherMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        A_Number = getValue("A_NUMBER");
        B_Number = getValue("B_NUMBER");
        C_Number = getValue("C_NUMBER");
        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                    if (B_Number != null) {
                        otherMSISDN = B_Number;
                    }
                    break;

                case "mtc":
                    if (A_Number != null) {
                        otherMSISDN = A_Number;
                    }
                    break;

                case "fwd":
                    if (C_Number != null) {
                        otherMSISDN = C_Number;
                    }
                    eventTypeKey = "5";
                    break;
            }

        if (otherMSISDN != null)
            return Optional.of(otherMSISDN);
        return Optional.empty();
    }

    String zeroDurationInd;

    public Optional<String> getZeroDurationInd() {
        zeroDurationInd = getValue("DURATION");

        if (zeroDurationInd != null) {
            return Optional.of(zeroDurationInd);
        }
        return Optional.empty();
    }

    String serviceID, chrgUnitIdKey;

    public Optional<String> getChrgUnitIdKey() {
        serviceID = getValue("SERVICEID");
        if (serviceID != null)
            switch (serviceID) {
                case "11":    //voice
                    chrgUnitIdKey = "3";
                    break;
                case "22":    //sms
                case "21":
                    chrgUnitIdKey = "10";
                    break;
            }
        if (chrgUnitIdKey != null) {
            return Optional.of(chrgUnitIdKey);
        }
        return Optional.empty();
    }

    String eventDirectionKey;

    public Optional<String> getEventDirectionKey() {
        callIndicator = getValue("CALLINDICATOR");
        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                case "fwd":
                    eventDirectionKey = "1";
                    break;

                case "mtc":
                    eventDirectionKey = "2";
                    break;
            }

        if (eventDirectionKey != null)
            return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    String eventTypeKey;

    public Optional<String> getEventTypeKey() {
        serviceID = getValue("SERVICEID");
        if (serviceID != null)
            switch (serviceID) {
                case "11":    //voice
                    eventTypeKey = "1";
                    break;
                case "22":    //sms
                case "21":
                    eventTypeKey = "2";
                    break;
                case "1F":    //video telephony
                    eventTypeKey = "13";
                    break;
            }
        if (eventTypeKey != null)
            return Optional.of(eventTypeKey);
        return Optional.empty();
    }

    String duration = null;
    long billablePulse = 0L;

    public Optional<Long> getBillablePulse() {
        duration = getValue("DURATION");
        if (duration != null) {
            try {
                long v = Long.parseLong(duration);
                billablePulse = (long) Math.ceil((double) v / 60);
                return Optional.of(billablePulse);
            } catch (Exception ignore) {
            }
        }
        return Optional.empty();
    }

    Date callStartTime;
    String genFullDate;

    String startDate, startTime, eventStartTime;

    public Optional<String> getStartTime() {
        startDate = getValue("STARTDATE");
        startTime = getValue("STARTTIME");
        eventStartTime = startDate.concat(startTime);
        try {
            if (eventStartTime != null) {
                callStartTime = sdfS.get().parse(eventStartTime);
                genFullDate = fullDate.get().format(callStartTime);
                return Optional.of(sdfT.get().format(callStartTime));
            }
        } catch (ParseException e) {
        }
        return Optional.empty();
    }

    Date callEndTime;
    int eventEndTime1 , startTime1 , duration1;

    String eventEndTime;

    public Optional<String> getEndTime() throws ParseException {
        startTime1 = Integer.parseInt(getValue("STARTTIME"));
        duration1 = Integer.parseInt(getValue("DURATION"));
        eventEndTime1 = startTime1 +duration1;

        eventEndTime = startDate+eventEndTime1;
        callEndTime = sdfS.get().parse(String.valueOf(eventEndTime));
        return Optional.of(sdfT.get().format(callEndTime));
    }

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }
}
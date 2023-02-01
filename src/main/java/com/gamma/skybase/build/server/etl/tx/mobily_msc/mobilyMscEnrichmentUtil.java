package com.gamma.skybase.build.server.etl.tx.mobily_msc;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimCRMSubscriber;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.gamma.telco.utility.TelcoBusinessTransformation.cache;

public class mobilyMscEnrichmentUtil {

    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
//    TelcoBusinessTransformation zz= new TelcoBusinessTransformation();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
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
            if (callIndicator.equalsIgnoreCase("fwd")) {
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

    String srvTypeKey , msrn;

    public int isPrepaid(String servedMSISDN) {
        String value;
        ReferenceDimCRMSubscriber subInfo = (ReferenceDimCRMSubscriber) cache.getRecord("DIM_CRM_INF_SUBSCRIBER_ALL", servedMSISDN);
        //todo fix it
        if (subInfo != null) {
            value = subInfo.getServedMsisdn();
            if (value != null) {
//                return Integer.parseInt(subInfo.getPrepaidFlag());//TODO
                return 0;
            }
        }
        return 1;
    }

    public Optional<String> getSrvTypeKey(){
        msrn = getValue("MSRN");
        boolean msrnFlag;
        if(msrn!=null) {
            msrnFlag = msrn.startsWith("966");
            int flag = isPrepaid(servedMSISDN);
            switch (flag){
                case 0:
                    if(msrnFlag){
                        srvTypeKey = "1";
                    }else {
                        srvTypeKey = "5";
                    }
                    break;
                case 1:
                    if(msrnFlag){
                        srvTypeKey = "2";
                    }else {
                        srvTypeKey = "6";
                    }
                    break;
                case 3:
                    if(msrnFlag){
                        srvTypeKey = "7";
                    }else {
                        srvTypeKey = "8";
                    }
                    break;
                default:
                    srvTypeKey = "-99";
                    break;
            }

            if (srvTypeKey != null)
                return Optional.of(srvTypeKey);
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

    LocalDateTime callStartTime;

    String genFullDate , startDate, startTime, eventStartTime;

    public Optional<String> getStartTime() {
        startDate = getValue("STARTDATE");
        startTime = getValue("STARTTIME");
        eventStartTime = startDate.concat(startTime);
        if (eventStartTime != null) {
            callStartTime = LocalDateTime.parse(eventStartTime, dtf2);
            genFullDate = callStartTime.toLocalDate().format(dtf1);
            return Optional.of(dtf.format(callStartTime));
        }
        return Optional.empty();
    }

    LocalDateTime callEndTime;
    int eventEndTime1, startTime1, duration1;

    String eventEndTime;

    public Optional<String> getEndTime() throws ParseException {
        startTime1 = Integer.parseInt(getValue("STARTTIME"));
        duration1 = Integer.parseInt(getValue("DURATION"));
        eventEndTime1 = startTime1 +duration1;

        eventEndTime = startDate+eventEndTime1;
        callEndTime = LocalDateTime.parse(eventEndTime,dtf2);
        return Optional.of(dtf.format(callEndTime));
    }


    public Optional<String> getServeMSRN(){
        return Optional.empty();
    }
    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }
}
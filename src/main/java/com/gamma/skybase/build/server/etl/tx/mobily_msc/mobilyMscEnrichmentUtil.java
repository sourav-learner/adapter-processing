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
import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

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

    public Optional<String> getServedMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        String A_Number = getValue("A_NUMBER");
        String B_Number = getValue("B_NUMBER");
        String C_Number = getValue("C_NUMBER");
        String aNumber = normalizeMSISDN(A_Number);
        String bNumber = normalizeMSISDN(B_Number);
        String cNumber = normalizeMSISDN(C_Number);

        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                case "emg":
                    if (aNumber != null) {
                        servedMSISDN = aNumber;
                    }
                    break;

                case "mtc":
                    if (bNumber != null) {
                        servedMSISDN = bNumber;
                    }
                    break;
                case "fwd":
                    if (cNumber != null){
                        servedMSRN = cNumber;
                    }
                    break;
                default:
                    break;
            }

        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String thirdPartyMSISDN;

    public Optional<String> getThirdPartyMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        String B_Number = getValue("B_NUMBER");
        String bNum = normalizeMSISDN(B_Number);

        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                case "mtc":
                    thirdPartyMSISDN = "-99";
                    break;
                case "fwd":
                    if (bNum != null) {
                        thirdPartyMSISDN = bNum;
                    }
                    break;
                default:
                    break;
            }

        if (thirdPartyMSISDN != null)
            return Optional.of(thirdPartyMSISDN);
        return Optional.empty();
    }

    String otherMSISDN;

    public Optional<String> getOtherMSISDN() {
        callIndicator = getValue("CALLINDICATOR");
        String A_Number = getValue("A_NUMBER");
        String B_Number = getValue("B_NUMBER");

        String aNums = normalizeMSISDN(A_Number);
        String bNums = normalizeMSISDN(B_Number);

        if (callIndicator != null)
            switch (callIndicator.toLowerCase()) {
                case "moc":
                case "emg":
                    if (bNums != null) {
                        otherMSISDN = bNums;
                    }
                    break;

                case "mtc":
                case "fwd":
                    if (aNums != null) {
                        otherMSISDN = aNums;
                    }
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

    String srvTypeKey, msrn;

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

    public Optional<String> getSrvTypeKey() {

        String flag = String.valueOf(isPrepaid(servedMSISDN));
        if (flag != null){
            switch (flag) {
                case "0":
                    srvTypeKey = "1";
                    break;
                case "1":
                    srvTypeKey = "2";
                    break;
                case "3":
                    srvTypeKey = "7";
                    break;
                default:
                    srvTypeKey = "-97";
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
                case "12":
                    chrgUnitIdKey = "3";
                    break;
                case "21":    //sms
                case "22":
                    chrgUnitIdKey = "10";
                    break;
                default:
                    chrgUnitIdKey = "-99";
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
                case "emg":
                    eventDirectionKey = "1";
                    break;

                case "mtc":
                    eventDirectionKey = "2";
                    break;
                default:
                    eventDirectionKey = "-99";
                    break;
            }

        if (eventDirectionKey != null)
            return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    String eventTypeKey;

    public Optional<String> getEventTypeKey() {
        serviceID = getValue("SERVICEID");
        callIndicator = getValue("CALLINDICATOR");
        if (serviceID != null)
            switch (serviceID) {
                case "11"://voice
                case "12":
                    eventTypeKey = "1";
                    break;
                case "22":    //sms
                case "21":
                    eventTypeKey = "2";
                    break;
                case "1F":    //video telephony
                    eventTypeKey = "13";
                    break;
                default:
                    break;
            }

        if (callIndicator != null){
            switch (callIndicator){
                case "SPL":
                    eventTypeKey = "-99";
                    break;
                case "FWD":
                    eventTypeKey = "5";
                    break;
                default:
                    break;
            }
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

    String genFullDate, startDate, startTime, eventStartTime;

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
        eventEndTime1 = startTime1 + duration1;

        eventEndTime = startDate + eventEndTime1;
        callEndTime = LocalDateTime.parse(eventEndTime, dtf2);
        return Optional.of(dtf.format(callEndTime));
    }

    String servedMSRN;

    public Optional<String> getServeMSRN() {
        msrn = getValue("MSRN");
        if (msrn != null) {
            servedMSRN = normalizeMSRN(msrn);
            return Optional.of(servedMSRN);
        }
        return Optional.empty();
    }

    String serveMSRNTest;

    public Optional<String> getServeMSRNTest() {
        msrn = getValue("MSRN");
        if (msrn != null) {
            serveMSRNTest = normalizeMSRNTest(msrn);
            return Optional.of(serveMSRNTest);
        }
        return Optional.empty();
    }
    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }

    String normalizeMSISDN(String number) {
        if (number != null){
            if (number.startsWith("0")) {
                number = ltrim(number, '0');
                if (number.length() < 10) {
                    number = "966" + number;
                }
            }
            if (number.length() < 10) {
                number = "966" + number;
            }
            return number;
        }
        return "";
    }

    String normalizeMSRN(String number) {
        if (number != null) {
            if (number.startsWith("966")) {
                return number;
            } else {
                number = number.substring(4);
                if (number.length() < 10) {
                    number = "966" + number;
                }
                return number;
            }
        }
        return "";
    }

    String normalizeMSRNTest(String number) {
        if (number != null) {
            if (number.startsWith("966")) {
                return number;
            } else {
                number = number.substring(2);
                if (number.length() < 10) {
                    number = "966" + number;
                }
                return number;
            }
        }
        return "";
    }
}
package com.gamma.skybase.build.server.etl.tx.med_sgsn;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimCRMSubscriber;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.gamma.telco.utility.TelcoBusinessTransformation.cache;
import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class medSGSNEnrichmentUtil {

    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LinkedHashMap<String, Object> rec;

    private medSGSNEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static medSGSNEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new medSGSNEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    LocalDateTime callStartTime;
    String genFullDate, callEventStartTimestamp;

    public Optional<String> getStartTime() {
        callEventStartTimestamp = getValue("CALL_EVENT_START_TIMESTAMP");
        if (callEventStartTimestamp != null) {
            callStartTime = LocalDateTime.parse(callEventStartTimestamp, dtf2);
            genFullDate = callStartTime.toLocalDate().format(dtf1);
            return Optional.of(dtf.format(callStartTime));
        }
        return Optional.empty();
    }

    String srvTypeKey;

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

    String servedMSISDN,msisdn;

    public Optional<String> getServedMSISDN() {
        msisdn = getValue("MSISDN");
        String msisdn1 = normalizeMSISDN(msisdn);
        if (msisdn1 != null)
            servedMSISDN = msisdn1;
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
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
                default:
                    srvTypeKey = "-99";
                    break;
            }
        }

        if (srvTypeKey != null)
            return Optional.of(srvTypeKey);

        return Optional.empty();
    }

    long totalVolume = 0L;
    Long downlinkVolume, uplinkVolume;

    public Optional<Long> getBillableBytes() {
        downlinkVolume = Long.valueOf(getValue("DATAVOLUMEINCOMING"));
        uplinkVolume = Long.valueOf(getValue("DATAVOLUMEOUTGOING"));

        downlinkVolume = Long.parseLong(String.valueOf(downlinkVolume));
        uplinkVolume = Long.parseLong(String.valueOf(uplinkVolume));

        if (downlinkVolume != null && uplinkVolume != null) {
            totalVolume = downlinkVolume + uplinkVolume;
        }

        Double bytes = Math.ceil((double) totalVolume / 1024);
        return Optional.of(bytes.longValue());
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

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }
}
package com.gamma.skybase.build.server.etl.decoder.med_ggsn;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class MedGGSNEnrichmentUtil extends LebaraUtil{
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LinkedHashMap<String, Object> rec;

    private MedGGSNEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static MedGGSNEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new MedGGSNEnrichmentUtil(record);
    }

    LocalDateTime callStartTime;
    String genFullDate , recordOpeningTime;

    public Optional<String> getStartTime() {
        recordOpeningTime = getValue("RECORD_OPENING_TIME");
        if (recordOpeningTime != null) {
            callStartTime = LocalDateTime.parse(recordOpeningTime, dtf2);
            genFullDate = callStartTime.toLocalDate().format(dtf1);
            return Optional.of(dtf.format(callStartTime));
        }
        return Optional.empty();
    }

    LocalDateTime callEndTime;

    String changeTime;
    public Optional<String> getEndTime() {
        changeTime = getValue("CHANGE_TIME");
        try {
            if (changeTime != null) {
                callEndTime = LocalDateTime.parse(changeTime , dtf2);
                return Optional.of(dtf.format(callEndTime));
            }
        }
        catch (Exception e){
        }
        return Optional.empty();
    }

    public Optional<String> getServedMSISDN() {
        String servedMsisdn, servedMSISDN;

        servedMsisdn = getValue("SERVED_MSISDN");
        servedMSISDN = normalizeMSISDN(servedMsisdn);
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String normalizeMSISDN(String number) {
        if (number != null) {
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

    ReferenceDimDialDigit getDialedDigitSettings(String servedMSISDN) {
        return txLib.getDialedDigitSettings(servedMSISDN);
    }

    String srvTypeKey;

    public Optional<String> getSrvTypeKey(String msisdn) {
        int flag = isPrepaid(msisdn);
        switch (flag) {
            case 0:
                srvTypeKey = "1";
                break;
            case 1:
                srvTypeKey = "2";
                break;
            case 3:
                srvTypeKey = "7";
                break;
            default:
                srvTypeKey = "-97";
                break;
        }
        return Optional.of(srvTypeKey);
    }

    public Optional<Long> getBillableBytes() {
        long totalVolume = 0L;
        String downlinkVolume = getValue("DATA_VOLUME_GPRS_DOWNLINK");
        String uplinkVolume = getValue("DATA_VOLUME_GPRS_UPLINK");

        if (downlinkVolume != null && uplinkVolume != null) {
            totalVolume = Long.parseLong(downlinkVolume + uplinkVolume);
        }
        Double bytes = Math.ceil((double) totalVolume / 1024);
        return Optional.of(bytes.longValue());
    }
}
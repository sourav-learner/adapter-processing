package com.gamma.skybase.build.server.etl.decoder.huw_sgsn_unl;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class HuwSGSNUnlEnrichmentUtil extends LebaraUtil {

    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private HuwSGSNUnlEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static HuwSGSNUnlEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new HuwSGSNUnlEnrichmentUtil(record);
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
    
    String servedMSISDN;
    
    public Optional<String> getServedMSISDN() {
        String msisdn;
        msisdn = getValue("MSISDN");
        String msisdn1 = normalizeMSISDN(msisdn);
        if (msisdn1 != null)
            servedMSISDN = msisdn1;
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    public Optional<String> getSrvTypeKey(String msisdn) {
        int flag = isPrepaid(msisdn);
        String srvTypeKey = null;
            switch (flag) {
                case 0:
                    srvTypeKey = "1";
                    break;
                case 1:
                    srvTypeKey = "2";
                    break;
                default:
                    srvTypeKey = "-99";
                    break;
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
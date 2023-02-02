package com.gamma.skybase.build.server.etl.tx.med_ggsn;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class medGGSNEnrichmentUtil {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LinkedHashMap<String, Object> rec;

    private medGGSNEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static medGGSNEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new medGGSNEnrichmentUtil(record);
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

    String dataVolumeGprsDownlink;
    Long downlinkVolume;

    public Optional<String> getDownloadVolume() {
        dataVolumeGprsDownlink = getValue("DATA_VOLUME_GPRS_DOWNLINK");
        if (dataVolumeGprsDownlink != null)
            try {
                downlinkVolume = Long.parseLong(dataVolumeGprsDownlink);
                return Optional.of(String.valueOf(downlinkVolume));
            } catch (NumberFormatException ignored) {
            }
        return Optional.empty();
    }

    String dataVolumeGprsUplink;
    Long uplinkVolume;

    public Optional<String> getUploadVolume() {
        dataVolumeGprsUplink = getValue("DATA_VOLUME_GPRS_UPLINK");
        if (dataVolumeGprsUplink != null)
            try {
                uplinkVolume = Long.parseLong(dataVolumeGprsUplink);
                return Optional.of(String.valueOf(uplinkVolume));
            } catch (NumberFormatException ignored) {
            }
        return Optional.empty();
    }
    long totalVolume = 0L;

    public Optional<Long> getBillableBytes() {
        if (downlinkVolume != null && uplinkVolume != null) {
            totalVolume = downlinkVolume + uplinkVolume;
        }
        Double bytes = Math.ceil((double) totalVolume / 1024);
        return Optional.of(bytes.longValue());
    }
}
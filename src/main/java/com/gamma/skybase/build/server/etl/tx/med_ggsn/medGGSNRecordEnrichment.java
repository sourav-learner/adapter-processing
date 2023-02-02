package com.gamma.skybase.build.server.etl.tx.med_ggsn;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class medGGSNRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        medGGSNEnrichmentUtil tx = medGGSNEnrichmentUtil.of(record);

//        EVENT_END_TIME
        Optional<String> callEndTime = tx.getEndTime();
        callEndTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

//        EVENT_START_TIME
        Optional<String> callStartTime = tx.getStartTime();
        callStartTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

//        DATA_VOLUME_GPRS_DOWNLINK
        Optional<String> dataVolumeIncoming = tx.getDownloadVolume();
        dataVolumeIncoming.ifPresent(s -> record.put("DATA_VOLUME_GPRS_DOWNLINK", s));

//        DATA_VOLUME_GPRS_UPLINK
        Optional<String> downloadVolume = tx.getUploadVolume();
        downloadVolume.ifPresent(s -> record.put("DATA_VOLUME_GPRS_UPLINK", s));

//        BILLABLE_VOLUME, ZERO_BYTE_IND
        Optional<Long> billableBytes = tx.getBillableBytes();
        billableBytes.ifPresent(s -> {
            record.put("BILLABLE_VOLUME", s);
            record.put("ZERO_BYTE_IND", s == 0 ? 1 : 0);
        });


//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

//        EVENT_DATE
        record.put("EVENT_DATE", tx.genFullDate);

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
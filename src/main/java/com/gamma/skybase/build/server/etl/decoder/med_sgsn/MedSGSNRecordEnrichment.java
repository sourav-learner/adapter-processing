package com.gamma.skybase.build.server.etl.decoder.med_sgsn;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class MedSGSNRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        MedSGSNEnrichmentUtil tx = MedSGSNEnrichmentUtil.of(record);


//        EVENT_START_TIME
        Optional<String> callStartTime = tx.getStartTime();
        callStartTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

//        BILLABLE_VOLUME, ZERO_BYTE_IND
        Optional<Long> billableBytes = tx.getBillableBytes();
        billableBytes.ifPresent(s -> {
            record.put("BILLABLE_VOLUME", s);
            record.put("ZERO_BYTE_IND", s == 0 ? 1 : 0);
        });

//        SERVED_MSISDN , SERVED_MSISDN_DIAL_DIGIT_KEY
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> {
            record.put("SERVED_MSISDN",s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
                    record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

//        SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey(record.get("SERVED_MSISDN").toString());
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY" ,s));

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
package com.gamma.skybase.build.server.etl.decoder.hur;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class HURRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> record = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();

        HUREnrichmentUtil tx = HUREnrichmentUtil.of(record);

        //        EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

//        XDR_DATE
        Optional<String> xdrDate = tx.getXdrDate();
        xdrDate.ifPresent(s -> record.put("XDR_DATE", s));

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
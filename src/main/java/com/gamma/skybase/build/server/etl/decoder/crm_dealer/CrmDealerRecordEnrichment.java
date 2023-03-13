package com.gamma.skybase.build.server.etl.decoder.crm_dealer;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CrmDealerRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CrmDealerEnrichmentUtil tx = CrmDealerEnrichmentUtil.of(record);

//        DEALER_STATUS
        Optional<String> dealerStatus = tx.getDealerStatus();
        dealerStatus.ifPresent(s -> record.put("DEALER_STATUS", s));

//        CREATE_DATE , XDR_DATE
        Optional<String> createDate = tx.getCreateDate();
        createDate.ifPresent(s ->{
            record.put("CREATEDATE", s);
            record.put("XDR_DATE", s);
        });

//        EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

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
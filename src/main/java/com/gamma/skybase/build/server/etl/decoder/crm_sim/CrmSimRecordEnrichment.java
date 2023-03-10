package com.gamma.skybase.build.server.etl.decoder.crm_sim;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CrmSimRecordEnrichment implements IEnrichment {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CrmSimEnrichmentUtil tx = CrmSimEnrichmentUtil.of(record);

        // IS_BIND
        Optional<String> isBind = tx.getIsBind();
        isBind.ifPresent(s -> record.put("ISBIND", s));
        //CREATE_DATE
        Optional<String> createDate = tx.getCreateDate();
        createDate.ifPresent(s -> record.put("CREATEDATE", s));

        // VALID_DATE
        Optional<String> validDate = tx.getValidDate();
        validDate.ifPresent(s -> record.put("VALIDDATE", s));

        //INVALID_DATE
        Optional<String> invalidDate = tx.getInvalidDate();
        invalidDate.ifPresent(s -> record.put("INVALIDDATE", s));

        // OPER_DATE
        Optional<String> operDate = tx.getOperDate();
        operDate.ifPresent(s -> record.put("OPERDATE", s));

        // IS_LOCKED
        Optional<String> isLocked = tx.getIsLocked();
        isLocked.ifPresent(s -> record.put("ISLOCKED", s));

        //EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

        // IS_RECYCLED
        Optional<String> isRecycled = tx.getIsRecycled();
        isRecycled.ifPresent(s -> record.put("ISRECYCLED", s));

        //ORDER_STATUS
        Optional<String> status = tx.getOrderStatus();
        status.ifPresent(s -> record.put("ORDERSTATUS", s));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        return data;
    }
}

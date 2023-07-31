package com.gamma.skybase.build.server.etl.decoder.uvc_cardstatus;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class UvccardstatusRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        UvccardstatusEnrichmentUtil tx = UvccardstatusEnrichmentUtil.of(record);

//        FACEVALUE
        Optional<String> faceValue = tx.getFaceValue();
        faceValue.ifPresent(s -> record.put("DENOMINATION", s));

//        HOT_CARD_FLAG_DESC
        Optional<String> hotCardFlagDesc = tx.getHotCardFlagDesc();
        hotCardFlagDesc.ifPresent(s -> record.put("HOT_CARD_FLAG_DESC", s));

//        XDR_DATE
        Optional<String> xdrDate = tx.getXdrDate();
        xdrDate.ifPresent(s -> record.put("XDR_DATE", s));

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        //EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
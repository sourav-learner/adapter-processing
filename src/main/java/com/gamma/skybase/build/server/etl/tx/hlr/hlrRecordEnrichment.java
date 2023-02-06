package com.gamma.skybase.build.server.etl.tx.hlr;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class hlrRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        hlrEnrichmentUtil tx = hlrEnrichmentUtil.of(record);

//        NETWORK_ACCESS_MODE
        Optional<String> networkAccessMode = tx.getNetworkAccessMode();
        networkAccessMode.ifPresent(s -> record.put("NETWORK_ACCESS_MODE", s));

//        ODB_INCOMING_CALL
        Optional<String> odbIncomingCall = tx.getOdbIncomingCall();
        odbIncomingCall.ifPresent(s -> record.put("ODB_INCOMING_CALL", s));

//        ODB_OUTGOING_CALL
        Optional<String> odbOutgoingCall = tx.getOdbOutgoingCall();
        odbOutgoingCall.ifPresent(s -> record.put("ODB_OUTGOING_CALL", s));


        // FILE_NAME , POPULATION_DATE , EVENT_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));
//        record.put("EVENT_DATE", tx.genFullDate);

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
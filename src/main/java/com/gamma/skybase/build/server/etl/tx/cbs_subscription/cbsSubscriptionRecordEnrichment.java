package com.gamma.skybase.build.server.etl.tx.cbs_subscription;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class cbsSubscriptionRecordEnrichment implements IEnrichment {

    private AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsSubscriptionEnrichmentUtil tx = cbsSubscriptionEnrichmentUtil.of(record);

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("STATUS", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("CUST_LOCAL_START_DATE", s);
            record.put("XDR_DATE", s);
        });

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_START_DATE", "CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("CUST_LOCAL_END_DATE", s));

        // OBJ_TYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJ_TYPE", s));

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULT_CODE", s));


        // FILE_NAME , POPULATION_DATE , EVENT_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));
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
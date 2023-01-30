package com.gamma.skybase.build.server.etl.tx.cbs_adj;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class cbsAdjRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsAdjEnrichmentUtil tx = cbsAdjEnrichmentUtil.of(record);

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

        // ENTRY_DATE
        Optional<String> starTime = tx.getStartTime("ENTRY_DATE");
        starTime.ifPresent(s -> {
            record.put("ENTRYDATE",s);
            record.put("XDR_DATE", s);
        });

//      SERVED_TYPE
        Optional<String> payType = tx.getServedType();
        payType.ifPresent(s -> record.put("SERVED_TYPE", s));

//        SERVED_MSISDN
        String priIdentity = tx.getValue("PRI_IDENTITY");
        if(priIdentity != null) {
            String servedMsisdn;
            if (priIdentity.length()<12){
                servedMsisdn = "966" + priIdentity;
            }
            else {
                servedMsisdn = priIdentity;
            }
            record.put("SERVED_MSISDN",servedMsisdn);
        }
        
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
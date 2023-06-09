package com.gamma.skybase.build.server.etl.decoder.cbs_subscription_cm;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class CbsSubscriptionCmRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsSubscriptionCmEnrichmentUtil tx = CbsSubscriptionCmEnrichmentUtil.of(record);

        //  CDR_STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

        // OBJTYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJTYPE", s));

        // RESULTCODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

//      SERVICE_CATEGORY
        String serviceCategory = tx.getValue("SERVICE_CATEGORY");
        record.put("SERVICE_CATEGORY1",serviceCategory);

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

        // SERVED_MSISDN
        String callingPartyNumber = tx.getValue("CallingPartyNumber");
        if(callingPartyNumber != null) {
            String servedMsisdn;
            if (callingPartyNumber.length()<12){
                servedMsisdn = "966" + callingPartyNumber;
            }
            else {
                servedMsisdn = callingPartyNumber;
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
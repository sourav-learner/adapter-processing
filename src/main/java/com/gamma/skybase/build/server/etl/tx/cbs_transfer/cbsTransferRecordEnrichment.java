package com.gamma.skybase.build.server.etl.tx.cbs_transfer;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class cbsTransferRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsTransferEnrichmentUtil tx = cbsTransferEnrichmentUtil.of(record);

        // RESULTCODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

        // TRANSFERTYPE
        Optional<String> transferType = tx.getTransferType();
        transferType.ifPresent(s -> record.put("TRANSFERTYPE", s));

        // TRANSFERDATE
        Optional<String> starTime = tx.getStartTime("TRANSFER_DATE");
        starTime.ifPresent(s -> {
            record.put("TRANSFERDATE",s);
            record.put("XDR_DATE", s);
        });

        // CDR_STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

        // SERVED_MSISDN
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
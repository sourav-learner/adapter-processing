package com.gamma.skybase.build.server.etl.tx.cbs_transfer;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class cbsTransferRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsTransferEnrichmentUtil tx = cbsTransferEnrichmentUtil.of(record);

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULT_CODE", s));

        // TRANSFER_TYPE
        Optional<String> transferType = tx.getTransferType();
        transferType.ifPresent(s -> record.put("TRANSFER_TYPE", s));

        // TRANSFER_DATE
        Optional<String> starTime = tx.getStartTime("TRANSFER_DATE");
        starTime.ifPresent(s -> {
            record.put("TRANSFER_DATE",s);
            record.put("XDR_DATE", s);
        });

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("STATUS", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

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

    private List<Map<String, Object>> getElementsList(List<Map<String, Object>> chargeInformation, String name) {
        List<Map<String, Object>> cd = new ArrayList<>();
        if (chargeInformation != null) {
            List<List<Map<String, Object>>> l = chargeInformation.stream()
                    .filter(e -> e.containsKey(name))
                    .map(e -> (List<Map<String, Object>>) e.get(name))
                    .collect(Collectors.toList());
            for (List<Map<String, Object>> i : l) cd.addAll(i);
        }
        return cd;
    }

}
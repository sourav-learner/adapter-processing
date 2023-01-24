package com.gamma.skybase.build.server.etl.tx.cbs_recurring;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class cbsRecurringRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsRecurringEnrichmentUtil tx = cbsRecurringEnrichmentUtil.of(record);

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULT_CODE", s));

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("STATUS", s));

        // CUST_LOCAL_START_DATE
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("CUST_LOCAL_START_DATE",s);
            record.put("XDR_DATE", s);
        });

//      CUST_LOCAL_END_DATE
        Optional<String> endTime = tx.getStartTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> {
            record.put("CUST_LOCAL_END_DATE",s);
        });

        // OBJ_TYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJ_TYPE", s));

//      CHARGING_PARTY_TYPE
        Optional<String> chargingPartyType = tx.getChargingPartyType();
        chargingPartyType.ifPresent(s -> record.put("ChargingPartyType", s));

//        CycleType
        Optional<String> cycleType = tx.getCycleType();
        cycleType.ifPresent(s -> record.put("CycleType", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

//      OrderStatus
        Optional<String> orderStatus = tx.getOrderStatus();
        orderStatus.ifPresent(s -> record.put("OrderStatus", s));

//      ChargePartyIndicator
        Optional<String> chargePartyIndicator = tx.getChargePartyIndicator();
        chargePartyIndicator.ifPresent(s -> record.put("ChargePartyIndicator", s));

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
}
package com.gamma.skybase.build.server.etl.decoder.cbs_recurring;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsRecurringRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsRecurringEnrichmentUtil tx = CbsRecurringEnrichmentUtil.of(record);

        // RESULTCODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

        //  CDR_STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("EVENT_START_TIME",s);
            record.put("XDR_DATE", s);
        });

//      EVENT_END_TIME
        Optional<String> endTime = tx.getStartTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("EVENT_END_TIME",s));

        // OBJTYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJTYPE", s));

//      CHARGING_PARTY_TYPE
        Optional<String> chargingPartyType = tx.getChargingPartyType();
        chargingPartyType.ifPresent(s -> record.put("CHARGING_PARTY_TYPE", s));

//        CYCLE_TYPE
        Optional<String> cycleType = tx.getCycleType();
        cycleType.ifPresent(s -> record.put("CYCLE_TYPE", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

//      ORDER_STATUS
        Optional<String> orderStatus = tx.getOrderStatus();
        orderStatus.ifPresent(s -> record.put("ORDER_STATUS", s));

//      CHARGE_PARTY_INDICATOR
        Optional<String> chargePartyIndicator = tx.getChargePartyIndicator();
        chargePartyIndicator.ifPresent(s -> record.put("CHARGE_PARTY_INDICATOR", s));

//        SERVED_MSISDN
        String chargingPartyNumber = tx.getValue("ChargingPartyNumber");
        if(chargingPartyNumber != null) {
            String servedMsisdn;
            if (chargingPartyNumber.length()<12){
                servedMsisdn = "966" + chargingPartyNumber;
            }
            else {
                servedMsisdn = chargingPartyNumber;
            }
            record.put("SERVED_MSISDN",servedMsisdn);
        }

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
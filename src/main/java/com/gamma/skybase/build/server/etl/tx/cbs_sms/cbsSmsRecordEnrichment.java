package com.gamma.skybase.build.server.etl.tx.cbs_sms;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class cbsSmsRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsSmsEnrichmentUtil tx = cbsSmsEnrichmentUtil.of(record);

        //  STATUS
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

        //  EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

        // CHARGING_TIME
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("CHARGING_TIME", s));

//        SEND_RESULT
        Optional<String> sendResult = tx.getSendResult();
        sendResult.ifPresent(s -> record.put("SEND_RESULT", s));

//        REFUND_INDICATOR
        Optional<String> refundIndicator = tx.getRefundIndicator();
        refundIndicator.ifPresent(s -> record.put("REFUND_INDICATOR", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

//      ON_NET_INDICATOR
        Optional<String> OnNetIndicator = tx.getOnNetIndicator();
        OnNetIndicator.ifPresent(s -> record.put("ON_NET_INDICATOR", s));

        //  ONLINE_CHARGING_FLAG
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("ONLINE_CHARGING_FLAG", s));

        //  START_TIME_OF_BILL_CYCLE
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("START_TIME_OF_BILL_CYCLE", s));

        //  GROUP_PAY_FLAG
        Optional<String> groupPayFlag = tx.getGroupPayFlag();
        groupPayFlag.ifPresent(s -> record.put("GROUP_PAY_FLAG", s));

        // CHARGE, ZERO_CHRG_IND
        Optional<String> charge = tx.getCharge("DEBIT_AMOUNT");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHARGE_IND", s.equals("0") ? "1" : "0");
        });

//        ZeroDurationInd
        AtomicInteger zeroDurationIndDefault = new AtomicInteger(1);
        Optional<String> zeroDurationInd = tx.getZeroDurationInd();
        zeroDurationInd.ifPresent(s -> {
            if (!s.equals("0")) zeroDurationIndDefault.set(0);
        });
        record.put("ZERO_DURATION_IND", zeroDurationIndDefault.get());

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

        //SERVICE_FLOW
        String serviceFlow = tx.getValue("ServiceFlow");
        record.put("SERVICE_FLOW", serviceFlow);

//      SERVICE_CATEGORY
        String serviceCategory = tx.getValue("SERVICE_CATEGORY");
        record.put("SERVICE_CATEGORY1",serviceCategory);

//        FILE_NAME , POPULATION_DATE , EVENT_DATE
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
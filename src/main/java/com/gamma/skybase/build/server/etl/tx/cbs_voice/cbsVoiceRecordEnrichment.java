package com.gamma.skybase.build.server.etl.tx.cbs_voice;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class cbsVoiceRecordEnrichment implements IEnrichment {

    private AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsVoiceEnrichmentUtil tx = cbsVoiceEnrichmentUtil.of(record);

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("STATUS", s));

        // OBJ_TYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJ_TYPE", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("CUST_LOCAL_START_DATE", s);
            record.put("XDR_DATE", s);
        });

        // FILE_NAME , POPULATION_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("CUST_LOCAL_END_DATE", s));

        //  EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("ServiceFlow", s));

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULT_CODE", s));

        // ChargingTime
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("ChargingTime", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("PayType", s));

        //  OnlineChargingFlag
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("OnlineChargingFlag", s));

        //  GroupPayFlag
        Optional<String> groupPayFlag = tx.getGroupPayFlag();
        groupPayFlag.ifPresent(s -> record.put("GroupPayFlag", s));

        //  StartTimeOfBillCycle
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("StartTimeOfBillCycle", s));

        // CHARGE, ZERO_CHRG_IND
        Optional<String> charge = tx.getCharge("DEBIT_AMOUNT");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHARGE_IND", s.equals("0") ? "1" : "0");
        });

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

//        SERVICE_CATEGORY
        String serviceCategory = tx.getValue("SERVICE_CATEGORY");
        record.put("SERVICE_CATEGORY1",serviceCategory);

        // SERVICE_FLOW
        String serviceFlow = tx.getValue("ServiceFlow");
        if(serviceFlow != null) {
            record.put("SERVICE_FLOW", serviceFlow);
            if ("1".equals(serviceFlow.trim())){
                 record.put("CallingPartyNumber",tx.getValue("CallingPartyNumber"));
                 record.put("CalledPartyNumber",tx.getValue("CalledPartyNumber"));
                 record.put("CallingPartyIMSI",tx.getValue("CallingPartyIMSI"));
                 record.put("CalledPartyIMSI",tx.getValue("CalledPartyIMSI"));
                 record.put("CallingCUGNo",tx.getValue("CallingCUGNo"));
                 record.put("CalledCUGNo",tx.getValue("CalledCUGNo"));
                 record.put("CallingRoamInfo",tx.getValue("CallingRoamInfo"));
                 record.put("CallingCellID",tx.getValue("CallingCellID"));
                 record.put("CalledRoamInfo",tx.getValue("CalledRoamInfo"));
                 record.put("CalledCellID",tx.getValue("CalledCellID"));
                 record.put("CallingHomeCountryCode",tx.getValue("CallingHomeCountryCode"));
                 record.put("CallingHomeAreaNumber",tx.getValue("CallingHomeAreaNumber"));
                 record.put("CallingHomeNetworkCode",tx.getValue("CallingHomeNetworkCode"));
                 record.put("CallingRoamCountryCode",tx.getValue("CallingRoamCountryCode"));
                 record.put("CallingRoamAreaNumber",tx.getValue("CallingRoamAreaNumber"));
                 record.put("CallingRoamNetworkCode",tx.getValue("CallingRoamNetworkCode"));
                 record.put("CalledHomeCountryCode",tx.getValue("CalledHomeCountryCode"));
                 record.put("CalledHomeAreaNumber",tx.getValue("CalledHomeAreaNumber"));
                 record.put("CalledHomeNetworkCode",tx.getValue("CalledHomeNetworkCode"));
                 record.put("CalledRoamCountryCode",tx.getValue("CalledRoamCountryCode"));
                 record.put("CalledRoamAreaNumber",tx.getValue("CalledRoamAreaNumber"));
                 record.put("CalledRoamNetworkCode",tx.getValue("CalledRoamNetworkCode"));
                 record.put("CallingNetworkType",tx.getValue("CallingNetworkType"));
                 record.put("CalledNetworkType",tx.getValue("CalledNetworkType"));
                 record.put("CallingVPNTopGroupNumber",tx.getValue("CallingVPNTopGroupNumber"));
                 record.put("CallingVPNGroupNumber",tx.getValue("CallingVPNGroupNumber"));
                 record.put("CallingVPNShortNumber",tx.getValue("CallingVPNShortNumber"));
                 record.put("CalledVPNTopGroupNumber",tx.getValue("CalledVPNTopGroupNumber"));
                 record.put("CalledVPNGroupNumber",tx.getValue("CalledVPNGroupNumber"));
                 record.put("CalledVPNShortNumber",tx.getValue("CalledVPNShortNumber"));
            }else{
                record.put("CalledPartyNumber",tx.getValue("CalledPartyNumber"));
                record.put("CallingPartyNumber",tx.getValue("CallingPartyNumber"));
                record.put("CalledPartyIMSI",tx.getValue("CalledPartyIMSI"));
                record.put("CallingPartyIMSI",tx.getValue("CallingPartyIMSI"));
                record.put("CalledCUGNo",tx.getValue("CalledCUGNo"));
                record.put("CallingCUGNo",tx.getValue("CallingCUGNo"));
                record.put("CalledNetworkType",tx.getValue("CalledNetworkType"));
                record.put("CallingNetworkType",tx.getValue("CallingNetworkType"));
                record.put("CalledRoamInfo",tx.getValue("CalledRoamInfo"));
                record.put("CalledCellID",tx.getValue("CalledCellID"));
                record.put("CallingRoamInfo",tx.getValue("CallingRoamInfo"));
                record.put("CallingCellID",tx.getValue("CallingCellID"));
                record.put("CalledHomeCountryCode",tx.getValue("CalledHomeCountryCode"));
                record.put("CalledHomeAreaNumber",tx.getValue("CalledHomeAreaNumber"));
                record.put("CalledHomeNetworkCode",tx.getValue("CalledHomeNetworkCode"));
                record.put("CalledRoamCountryCode",tx.getValue("CalledRoamCountryCode"));
                record.put("CalledRoamAreaNumber",tx.getValue("CalledRoamAreaNumber"));
                record.put("CalledRoamNetworkCode",tx.getValue("CalledRoamNetworkCode"));
                record.put("CallingHomeCountryCode",tx.getValue("CallingHomeCountryCode"));
                record.put("CallingHomeAreaNumber",tx.getValue("CallingHomeAreaNumber"));
                record.put("CallingHomeNetworkCode",tx.getValue("CallingHomeNetworkCode"));
                record.put("CallingRoamCountryCode",tx.getValue("CallingRoamCountryCode"));
                record.put("CallingRoamAreaNumber",tx.getValue("CallingRoamAreaNumber"));
                record.put("CallingRoamNetworkCode",tx.getValue("CallingRoamNetworkCode"));
                record.put("CalledVPNTopGroupNumber",tx.getValue("CalledVPNTopGroupNumber"));
                record.put("CalledVPNGroupNumber",tx.getValue("CalledVPNGroupNumber"));
                record.put("CalledVPNShortNumber",tx.getValue("CalledVPNShortNumber"));
                record.put("CallingVPNTopGroupNumber",tx.getValue("CallingVPNTopGroupNumber"));
                record.put("CallingVPNGroupNumber",tx.getValue("CallingVPNGroupNumber"));
                record.put("CallingVPNShortNumber",tx.getValue("CallingVPNShortNumber"));
            }
        }

//        ZeroDurationInd
        AtomicInteger zeroDurationIndDefault = new AtomicInteger(1);
        Optional<String> zeroDurationInd = tx.getZeroDurationInd();
        zeroDurationInd.ifPresent(s -> {
            if (!s.equals("0")) zeroDurationIndDefault.set(0);
        });
        record.put("ZERO_DURATION_IND", zeroDurationIndDefault.get());

        //  EVENT_DATE
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
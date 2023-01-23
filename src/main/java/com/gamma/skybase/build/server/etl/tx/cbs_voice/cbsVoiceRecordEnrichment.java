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
                 record.put("SERVED_MSISDN",tx.getValue("CallingPartyNumber"));
                 record.put("OTHER_MSISDN",tx.getValue("CalledPartyNumber"));
                 record.put("SERVED_IMSI",tx.getValue("CallingPartyIMSI"));
                 record.put("OTHER_IMSI",tx.getValue("CalledPartyIMSI"));
                 record.put("SERVED_CUG",tx.getValue("CallingCUGNo"));
                 record.put("OTHER_CUG",tx.getValue("CalledCUGNo"));
                 record.put("SERVED_PLMN",tx.getValue("CallingRoamInfo"));
                 record.put("SERVED_CELL",tx.getValue("CallingCellID"));
                 record.put("OTHER_PLMN",tx.getValue("CalledRoamInfo"));
                 record.put("OTHER_CELL",tx.getValue("CalledCellID"));
                 record.put("SERVED_CC",tx.getValue("CallingHomeCountryCode"));
                 record.put("SERVED_AREA_CODE",tx.getValue("CallingHomeAreaNumber"));
                 record.put("SERVED_NETWORK_CODE",tx.getValue("CallingHomeNetworkCode"));
                 record.put("SERVED_ROAMING_CC",tx.getValue("CallingRoamCountryCode"));
                 record.put("SERVED_ROAM_AREA_CODE",tx.getValue("CallingRoamAreaNumber"));
                 record.put("SERVED_ROAM_NETWORK_CODE",tx.getValue("CallingRoamNetworkCode"));
                 record.put("OTHER_CC",tx.getValue("CalledHomeCountryCode"));
                 record.put("OTHER_AREA_CODE",tx.getValue("CalledHomeAreaNumber"));
                 record.put("OTHER_NETWORK_CODE",tx.getValue("CalledHomeNetworkCode"));
                 record.put("OTHER_ROAM_CC",tx.getValue("CalledRoamCountryCode"));
                 record.put("OTHER_ROAM_AREA_CODE",tx.getValue("CalledRoamAreaNumber"));
                 record.put("OTHER_ROAM_NETWORK_CODE",tx.getValue("CalledRoamNetworkCode"));
                 record.put("SERVED_NW_TYPE",tx.getValue("CallingNetworkType"));
                 record.put("OTHER_NW_TYPE",tx.getValue("CalledNetworkType"));
                 record.put("SERVED_VPN_TOP_GROUP_NUMBER",tx.getValue("CallingVPNTopGroupNumber"));
                 record.put("SERVED_VPN_GROUP_NUMBER",tx.getValue("CallingVPNGroupNumber"));
                 record.put("SERVED_VPN_SHORT_NUMBER",tx.getValue("CallingVPNShortNumber"));
                 record.put("OTHER_VPN_TOP_GROUP_NUMBER",tx.getValue("CalledVPNTopGroupNumber"));
                 record.put("OTHER_VPN_GROUP_NUMBER",tx.getValue("CalledVPNGroupNumber"));
                 record.put("OTHER_VPN_SHORT_NUMBER",tx.getValue("CalledVPNShortNumber"));
            }
            else if ("2".equals(serviceFlow.trim())){
                record.put("SERVED_MSISDN",tx.getValue("CalledPartyNumber"));
                record.put("OTHER_MSISDN",tx.getValue("CallingPartyNumber"));
                record.put("SERVED_IMSI",tx.getValue("CalledPartyIMSI"));
                record.put("OTHER_IMSI",tx.getValue("CallingPartyIMSI"));
                record.put("SERVED_CUG",tx.getValue("CalledCUGNo"));
                record.put("OTHER_CUG",tx.getValue("CallingCUGNo"));
                record.put("SERVED_NW_TYPE",tx.getValue("CalledNetworkType"));
                record.put("OTHER_NW_TYPE",tx.getValue("CallingNetworkType"));
                record.put("SERVED_PLMN",tx.getValue("CalledRoamInfo"));
                record.put("SERVED_CELL",tx.getValue("CalledCellID"));
                record.put("OTHER_PLMN",tx.getValue("CallingRoamInfo"));
                record.put("OTHER_CELL",tx.getValue("CallingCellID"));
                record.put("SERVED_CC",tx.getValue("CalledHomeCountryCode"));
                record.put("SERVED_AREA_CODE",tx.getValue("CalledHomeAreaNumber"));
                record.put("SERVED_NETWORK_CODE",tx.getValue("CalledHomeNetworkCode"));
                record.put("SERVED_ROAMING_CC",tx.getValue("CalledRoamCountryCode"));
                record.put("SERVED_ROAM_AREA_CODE",tx.getValue("CalledRoamAreaNumber"));
                record.put("SERVED_ROAM_NETWORK_CODE",tx.getValue("CalledRoamNetworkCode"));
                record.put("OTHER_CC",tx.getValue("CallingHomeCountryCode"));
                record.put("OTHER_AREA_CODE",tx.getValue("CallingHomeAreaNumber"));
                record.put("OTHER_NETWORK_CODE",tx.getValue("CallingHomeNetworkCode"));
                record.put("OTHER_ROAM_CC",tx.getValue("CallingRoamCountryCode"));
                record.put("OTHER_ROAM_AREA_CODE",tx.getValue("CallingRoamAreaNumber"));
                record.put("OTHER_ROAM_NETWORK_CODE",tx.getValue("CallingRoamNetworkCode"));
                record.put("SERVED_VPN_TOP_GROUP_NUMBER",tx.getValue("CalledVPNTopGroupNumber"));
                record.put("SERVED_VPN_GROUP_NUMBER",tx.getValue("CalledVPNGroupNumber"));
                record.put("SERVED_VPN_SHORT_NUMBER",tx.getValue("CalledVPNShortNumber"));
                record.put("OTHER_VPN_TOP_GROUP_NUMBER",tx.getValue("CallingVPNTopGroupNumber"));
                record.put("OTHER_VPN_GROUP_NUMBER",tx.getValue("CallingVPNGroupNumber"));
                record.put("OTHER_VPN_SHORT_NUMBER",tx.getValue("CallingVPNShortNumber"));
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
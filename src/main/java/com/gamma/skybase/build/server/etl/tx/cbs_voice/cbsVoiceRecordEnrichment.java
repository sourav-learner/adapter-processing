package com.gamma.skybase.build.server.etl.tx.cbs_voice;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class cbsVoiceRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsVoiceEnrichmentUtil tx = cbsVoiceEnrichmentUtil.of(record);

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

        // OBJ_TYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJTYPE", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

        // FILE_NAME , POPULATION_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

        //  EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

        // CHARGING_TIME
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("CHARGING_TIME", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("SERVED_TYPE", s));

        //  ONLINE_CHARGING_FLAG
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("ONLINE_CHARGING_FLAG", s));

        //  GROUP_PAY_FLAG
        Optional<String> groupPayFlag = tx.getGroupPayFlag();
        groupPayFlag.ifPresent(s -> record.put("GROUP_PAY_FLAG", s));

        //  START_TIME_OF_BILL_CYCLE
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("START_TIME_OF_BILL_CYCLE", s));

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
}
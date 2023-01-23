package com.gamma.skybase.build.server.etl.tx.cbs_gprs;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class cbsGprsRecordEnrichment implements IEnrichment {

    private AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        cbsGprsEnrichmentUtil tx = cbsGprsEnrichmentUtil.of(record);

        //  STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("STATUS", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("CUST_LOCAL_START_DATE", s);
            record.put("XDR_DATE", s);
        });

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("CUST_LOCAL_END_DATE", s));

        // OBJ_TYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJ_TYPE", s));

        // RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULT_CODE", s));

        //  EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("ServiceFlow", s));

        // ChargingTime
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("ChargingTime", s));

//      SERVED_TYPE
        Optional<String> servedType = tx.getServedType();
        servedType.ifPresent(s -> record.put("PayType", s));

        //  OnlineChargingFlag
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("OnlineChargingFlag", s));

        //  StartTimeOfBillCycle
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("StartTimeOfBillCycle", s));

        // CHARGE, ZERO_CHRG_IND
        Optional<Double> charge = tx.getCharge("DEBIT_AMOUNT");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHARGE_IND", s == 0 ? "1" : "0");
        });

        // UploadVolume
        Optional<Long> uploadVolume = tx.getUploadVolume("UpFlux");
        uploadVolume.ifPresent(s -> record.put("COMPUTED_UPLOAD_VOL", s));
        String UpFlux = tx.getValue("UpFlux");
        record.put("UpFlux", UpFlux);

        // DownloadVolume
        Optional<Long> downloadVolume = tx.getDownloadVolume("DownFlux");
        downloadVolume.ifPresent(s -> record.put("COMPUTED_DOWNLOAD_VOL", s));
        String DownFlux = tx.getValue("DownFlux");
        record.put("DownFlux", DownFlux);

        // TotalFlux
        Optional<Long> totalFlux = tx.getTotalVol("TotalFlux");
        totalFlux.ifPresent(s -> record.put("COMPUTED_TOTAL_VOL", s));
        String TotalFlux = tx.getValue("TotalFlux");
        record.put("TotalFlux", TotalFlux);

//      SERVICE_CATEGORY
        String serviceCategory = tx.getValue("SERVICE_CATEGORY");
        record.put("SERVICE_CATEGORY1",serviceCategory);

        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        // POPULATION_DATE_TIME , EVENT_DATE
        record.put("POPULATION_DATE_TIME", tx.sdfT.get().format(new Date()));
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
package com.gamma.skybase.build.server.etl.tx.nrtrde;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class NRTRDERecordEnrichment1 implements IEnrichment {

    private AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        NRTRDEEnrichmentUtil tx = NRTRDEEnrichmentUtil.of(record);

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("callEventStartTimeStamp");
        starTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        // CALL_DURATION, ZERO_DURATION_IND
        Optional<String> callEventDuration = tx.getCallDuration("callEventDuration");
        callEventDuration.ifPresent(s -> {
            String val = callEventDuration.get();
            record.put("CALL_DURATION", val);
            record.put("ZERO_DURATION_IND", val.equals("0") ? "1" : "0");
        });

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("callEventStartTimeStamp", "callEventDuration");
        endTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

        // NODE_ADDRESS
        Optional<String> nodeAddress = tx.getNodeAddress("fileName");
        nodeAddress.ifPresent(s -> record.put("NODE_ADDRESS", s));

        // BASIC_SERVICE_KEY
        Optional<String> basicServiceKey = tx.getBasicServiceKey("bearerServiceCode", "teleServiceCode");
        basicServiceKey.ifPresent(s -> record.put("BASIC_SERVICE_KEY", s));

        // EVENT_TYPE_KEY
        Optional<String> eventTypeKey = tx.getEventTypeKey();
        eventTypeKey.ifPresent(s -> record.put("EVENT_TYPE_KEY", s));

        // EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

        // SRV_TYPE_KEY
        Optional<String> servType = tx.getServType();
        servType.ifPresent(s -> record.put("SRV_TYPE_KEY", s));

        // BILLABLE_PULSE
        Optional<Long> billablePulse = tx.getBillablePulse("originalDuration");
        billablePulse.ifPresent(s -> {
            record.put("BILLABLE_PULSE", s);
        });

        // ORIGINAL_A_NUM
        record.put("ORIGINAL_A_NUM", record.get("msisdn"));

        // ORIGINAL_B_NUM
        Optional<String> originalBNumber = tx.getOriginalBNumber();
        originalBNumber.ifPresent(s -> record.put("ORIGINAL_B_NUM", s));

        // OTHER_MSISDN related lookup info
        Optional<String> otherMSISDN = tx.getOtherMSISDN();
        otherMSISDN.ifPresent(s -> {
            record.put("OTHER_MSISDN", s);
            ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
            if (ddk != null) {
                record.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                record.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                record.put("EVENT_CATEGORY_KEY", ddk.getEventCategoryKey());
                record.put("OTHER_MSISDN_ISO_CODE", ddk.getIsoCountryCode());
                record.put("OTHER_OPER", ddk.getProviderDesc());

                // FLEXI_IND_2
                String cd = ddk.getTargetCountryDesc();
                String iso = ddk.getIsoCountryCode();
                Optional<String> flex2 = tx.getFlexInd2(cd, iso);
                flex2.ifPresent(f -> record.put("FLEXI_IND_2", f));
            }
        });

        // CHARGE, ZERO_CHRG_IND
        Optional<String> charge = tx.getCharge("chargeAmount");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHRG_IND", s.equals("0") ? "1" : "0");
        });

        // CHRG_UNIT_ID_KEY
        Optional<String> chargeUnitIDKey = tx.getChargeUnitIDKey();
        chargeUnitIDKey.ifPresent(s -> record.put("CHRG_UNIT_ID_KEY", s));

        // ORIGINAL_DOWNLINK_VOLUME
        Optional<String> dataVolumeIncoming = tx.getDownloadVolume();
        dataVolumeIncoming.ifPresent(s -> record.put("ORIGINAL_UPLINK_VOLUME", s));

        // ORIGINAL_UPLINK_VOLUME
        Optional<String> downloadVolume = tx.getUploadVolume();
        downloadVolume.ifPresent(s -> record.put("ORIGINAL_UPLINK_VOLUME", s));

        // BILLABLE_BYTES, ZERO_BYTE_IND
        Optional<Long> billableBytes = tx.getBillableBytes();
        billableBytes.ifPresent(s -> {
            record.put("BILLABLE_BYTES", s);
            record.put("ZERO_BYTE_IND", s == 0 ? 1 : 0);
        });

        // POPULATION_DATE_TIME , UPDATE_DATE_TIME , EVENT_DATE
        record.put("POPULATION_DATE_TIME", tx.sdfT.get().format(new Date()));
        record.put("UPDATE_DATE_TIME", tx.sdfT.get().format(new Date()));

        record.put("EVENT_DATE", tx.genFullDate);

        // CDR_TYPE, partnerCountryName, partnerCountryISO, partnerOper
        Map<String, Object> basicInfo = tx.getNRTRDEBasicInfo();
        record.putAll(basicInfo);

        // SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey();
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY", s));

        // EDR_ID_KEY
        record.put("EDR_ID_KEY", tx.getEDRIdKey());

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
package com.gamma.skybase.build.server.etl.tx.med_tapin;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class medTAPINRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        medTAPINEnrichmentUtil tx = medTAPINEnrichmentUtil.of(record);

//        SERVED_MSISDN , SERVED_MSISDN_DIAL_DIGIT_KEY
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> {
            record.put("SERVED_MSISDN",s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
                    record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

//        OTHER_MSISDN
        Optional<String> otherMSISDN = tx.getOtherMSISDN();
        otherMSISDN.ifPresent(s -> {
            record.put("OTHER_MSISDN",s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
                    record.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                    record.put("OTHER_MSISDN_OPER", ddk.getProviderDesc());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

//        EVENT_START_TIME
        Optional<String> eventStartTime = tx.getEventStartTime("CALL_TIMESTAMP");
        eventStartTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

//      TAP_RATED_DATE
        Optional<String> tapRatedDate = tx.getTapRatedDate("TAP_RATED_DATE");
        tapRatedDate.ifPresent(s -> record.put("TAP_RATED_DATE", s));

//        VOLUME_OUTGOING
        Optional<String> volumeOutgoing = tx.getVolumeOutgoing();
        volumeOutgoing.ifPresent(s -> record.put("VOLUME_OUTGOING", s));

//        TOTAL_VOLUME
        Optional<String> totalVolume = tx.getTotalVolume();
        totalVolume.ifPresent(s -> record.put("TOTAL_VOLUME", s));

//        VOLUME_INCOMING
        Optional<String> volumeIncoming = tx.getVolumeIncoming();
        volumeIncoming.ifPresent(s -> record.put("VOLUME_INCOMING", s));

//        SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey();
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY" ,s));

//       EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

//        CHRG_UNIT_ID_KEY
        Optional<String> chrgUnitIdKey = tx.getChrgUnitIdKey();
        chrgUnitIdKey.ifPresent(s -> record.put("CHRG_UNIT_ID_KEY", s));

//        EVENT_TYPE_KEY
        Optional<String> eventTypeKey = tx.getEventTypeKey();
        eventTypeKey.ifPresent(s -> record.put("EVENT_TYPE_KEY", s));

//        BILLABLE_PULSE , ZERO_DUR_IND
        Optional<Long> billablePulse = tx.getBillablePulse();
        billablePulse.ifPresent(s ->{
            record.put("BILLABLE_PULSE", s);
            record.put("ZERO_DUR_IND", s.equals("0") ? "0" : "1");
        });

//        GPRS_UPLINK_VOLUME
        Optional<String> uplinkVolume = tx.getGprsUplinkVolume();
        uplinkVolume.ifPresent(s -> record.put("GPRS_UPLINK_VOLUME", s));

//        GPRS_DOWNLINK_VOLUME
        Optional<String> downLinkVolume = tx.getGprsDownlinkVolume();
        downLinkVolume.ifPresent(s -> record.put("GPRS_DOWNLINK_VOLUME", s));

//        GPRS_TOTAL_VOLUME
        Optional<String> total = tx.getGprsTotalVolume();
        total.ifPresent(s -> record.put("GPRS_TOTAL_VOLUME", s));

//        partnerCountryName , partnerOper
        Map<String, Object> basicInfo = tx.getTAPINBasicInfo();
        record.putAll(basicInfo);

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

//        EVENT_DATE
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
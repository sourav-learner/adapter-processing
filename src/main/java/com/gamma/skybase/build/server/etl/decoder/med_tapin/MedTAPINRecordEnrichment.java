package com.gamma.skybase.build.server.etl.decoder.med_tapin;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class MedTAPINRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        MedTAPINEnrichmentUtil tx = MedTAPINEnrichmentUtil.of(record);

//        SERVED_MSISDN , SERVED_MSISDN_DIAL_DIGIT_KEY
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> {
            record.put("SERVED_MSISDN", s);
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
            record.put("OTHER_MSISDN", s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
                    record.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                    record.put("OTHER_MSISDN_OPER", ddk.getProviderDesc());
                    String providerDesc = ddk.getProviderDesc();
                    record.put("OTHER_PARTY_OPERATOR" , providerDesc);
                    String targetCountryCode = ddk.getTargetCountryCode();
                    String otherPartyNwIndKey = "";
                    if (targetCountryCode.equals("966")){
                        switch (providerDesc){
                            case "GSM-Lebara Mobile":
                            case "LEBARA- Free Number":
                            case "LEBARA-Spl Number":
                                otherPartyNwIndKey = "1";
                                break;
                            default:
                                otherPartyNwIndKey = "2";
                                break;
                        }
                    }
                    else if (!targetCountryCode.equals("966")){
                        otherPartyNwIndKey = "3";
                    }
                    else {
                        otherPartyNwIndKey = "-99";
                    }
                    record.put("OTHER_PARTY_NW_IND_KEY" , otherPartyNwIndKey);
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
        Optional<String> srvTypeKey = tx.getSrvTypeKey(record.get("SERVED_MSISDN").toString());
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY", s));

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
        billablePulse.ifPresent(s -> {
            record.put("BILLABLE_PULSE", s);
            record.put("ZERO_DUR_IND", s == 0 ? "1" : "0");
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

//        EVENT_END_TIME
        Optional<String> eventEndTime;
        try {
            eventEndTime = tx.getEndTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        eventEndTime.ifPresent(s -> record.put("EVENT_END_TIME", s));


//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE", dtf.format(ldt));

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
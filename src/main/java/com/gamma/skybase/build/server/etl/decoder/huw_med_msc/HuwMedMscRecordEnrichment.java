package com.gamma.skybase.build.server.etl.decoder.huw_med_msc;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class HuwMedMscRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        HuwMedMscEnrichmentUtil tx = HuwMedMscEnrichmentUtil.of(record);

//        SERVED_MSISDN
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> {
            record.put("SERVED_MSISDN", s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
//                    record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
//                    record.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                }
            } catch (Exception e) {
//                e.printStackTrace();
            }
        });

//        THIRD_PARTY_MSISDN
        Optional<String> thirdPartyMSISDN = tx.getThirdPartyMSISDN();
        thirdPartyMSISDN.ifPresent(s -> {
            record.put("THIRD_PARTY_MSISDN", s);
            try {
                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
                if (ddk != null) {
                    record.put("THIRD_PARTY_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                }
            } catch (Exception e) {
//                e.printStackTrace();
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
                    String providerDesc = ddk.getProviderDesc();
                    record.put("OTHER_PARTY_OPERATOR" , providerDesc);
                    String targetCountryCode = ddk.getTargetCountryCode();
                    String otherPartyNwIndKey = "";
                    if (targetCountryCode.equals("966")){
                        switch (providerDesc){
                            case "GSM-Lebara Mobile":
                            case "LEBARA- Free Number":
                            case "LEBARA-Spl Number":
                            case "LEBARA-VAS":
                            case "LEBARA -M2M":
                            case "LEBARA-B2B-DATA-NUMBER":
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
//                e.printStackTrace();
            }
        });

//      SERVED_MRSN
//        Optional<String> serveMSRN = tx.getServeMSRN();
//        serveMSRN.ifPresent(s -> {
//            record.put("SERVED_MRSN", s);
//            try {
//                ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(s);
//                if (ddk != null) {
//                    record.put("SERVED_MSRN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
//                }
//            } catch (Exception e) {
////                e.printStackTrace();
//            }
//        });

//      SERVED_MRSN_TEST
//        Optional<String> serveMSRNTest = tx.getServeMSRNTest();
//        serveMSRNTest.ifPresent(s -> record.put("SERVED_MRSN_TEST", s));

//        SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey(record.get("SERVED_MSISDN").toString());
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY" ,s));

//      CHRG_UNIT_ID_KEY
        Optional<String> chrgUnitIdKey = tx.getChrgUnitIdKey();
        chrgUnitIdKey.ifPresent(s -> record.put("CHRG_UNIT_ID_KEY", s));

//       EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

//        EVENT_TYPE_KEY
        Optional<String> eventTypeKey = tx.getEventTypeKey();
        eventTypeKey.ifPresent(s -> record.put("EVENT_TYPE_KEY", s));

//        ZERO_DURATION_IND
        AtomicInteger zeroDurationIndDefault = new AtomicInteger(1);
        Optional<String> zeroDurationInd = tx.getZeroDurationInd();
        zeroDurationInd.ifPresent(s -> {
            if (!s.equals("0")) zeroDurationIndDefault.set(0);
        });
        record.put("ZERO_DURATION_IND", zeroDurationIndDefault.get());

//        BILLABLE_PULSE
        Optional<Long> billablePulse = tx.getBillablePulse();
        billablePulse.ifPresent(s -> record.put("BILLABLE_PULSE", s));

//        EVENT_START_TIME , XDR_DATE
        Optional<String> eventStartTime = tx.getStartTime();
        eventStartTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

//        EVENT_END_TIME
        Optional<String> eventEndTime;
        try {
            eventEndTime = tx.getEndTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        eventEndTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

//        FILE_NAME , EVENT_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("EVENT_DATE", tx.genFullDate);

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
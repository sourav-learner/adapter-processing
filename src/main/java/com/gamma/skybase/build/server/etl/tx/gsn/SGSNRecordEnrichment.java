package com.gamma.skybase.build.server.etl.tx.gsn;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SGSNRecordEnrichment implements IEnrichment {

    private OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private AppConfig appConfig = AppConfig.instance();
    private String opcoCode = appConfig.getProperty("app.datasource.opcode");
    private static final Logger logger = LoggerFactory.getLogger(SGSNRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final ThreadLocal<Calendar> cal = ThreadLocal.withInitial(Calendar::getInstance);


    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if(data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        }
        else response.setResponseCode(false);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {

        try {

            Object servedIMSI = record.get("servedIMSI");
            if(servedIMSI != null){
                if(servedIMSI.toString().startsWith(opcoCode)) return null;
            }

            Object originalMsisdn = record.get("servedMSISDN");
            record.put("ORIGINAL_A_NUM", originalMsisdn);
            String servedMSISDN = "";
            if (originalMsisdn != null && !originalMsisdn.equals("")) {
                servedMSISDN = originalMsisdn.toString().substring(2);
                record.put("SERVED_MSISDN", servedMSISDN);
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN);
                if (ddk != null) {
                    record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                    record.put("SERVED_MSISDN_OPER", ddk.getProviderDesc());
                    record.put("SERVED_MSISDN_ISO_CODE", ddk.getIsoCountryCode());
                }
            }

            Object accessPointNameNI = record.get("accessPointNameNI");
            Object accessPointNameOI = record.get("accessPointNameOI");
            if (accessPointNameNI != null) {
                accessPointNameNI = accessPointNameNI.toString().trim();
                record.put("accessPointNameNI", accessPointNameNI);
            }

            String EVENT_TYPE_KEY = "4";
            if (accessPointNameNI.toString().equalsIgnoreCase("mms")) EVENT_TYPE_KEY = "20";
            record.put("EVENT_TYPE_KEY", EVENT_TYPE_KEY);
            record.put("EVENT_DIRECTION_KEY", "1");


            String REC_TYPE_ID_KEY;
            Object recordSequenceNumber = record.get("recordSequenceNumber");
            if (recordSequenceNumber == null) {
                REC_TYPE_ID_KEY = "4";
            } else if (recordSequenceNumber.toString().equals("1")) {
                REC_TYPE_ID_KEY = "5";
            } else {
                Object causeForRecClosing = record.get("causeForRecClosing");
                if (causeForRecClosing.toString().equals("0") || causeForRecClosing.toString().equals("1")) {
                    REC_TYPE_ID_KEY = "7";
                } else {
                    REC_TYPE_ID_KEY = "6";
                }
            }
            record.put("REC_TYPE_ID_KEY", REC_TYPE_ID_KEY);

            // EVENT_START_TIME;
            Object recordOpeningTime = record.get("recordOpeningTime");
            if (recordOpeningTime != null) {
                try {
                    Date f = sdfS.get().parse(recordOpeningTime.toString());
                    record.put("recordOpeningTime", sdfT.get().format(f));
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            String EVENT_START_TIME = null;
            Date eventStartTime = null;
            try {
                if (recordOpeningTime == null) {
                    recordOpeningTime = first((List<Map<String, Object>>) record.get("changeOfServiceCondition"), "timeOfFirstUsage");
                }
                eventStartTime = sdfS.get().parse(recordOpeningTime.toString());
                EVENT_START_TIME = sdfT.get().format(eventStartTime);

                record.put("GENERATED_FULL_DATE", new SimpleDateFormat("yyyyMMdd").format(eventStartTime) + " 00:00:00"); //31
                record.put("EVENT_START_TIME_SLOT_KEY", new SimpleDateFormat("yyyyMMdd HH").format(eventStartTime) + ":00:00");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            record.put("EVENT_START_TIME", EVENT_START_TIME); // 2
            record.put("XDR_DATE", EVENT_START_TIME); // 2
            Date currentTime = new Date();
            record.put("POPULATION_DATE_TIME", sdfT.get().format(currentTime));

            if (record.containsKey("changeOfCharCondition")){
                logger.info("changeOfCharCondition key found");
            }

            Object listOfTrafficVolumes = record.get("listOfTrafficVolumes");
            Object changeTime;
            if (listOfTrafficVolumes != null) {
                if (listOfTrafficVolumes instanceof List && !((List) listOfTrafficVolumes).isEmpty()){
                    List<Map<String, Object>> trafficVolumeItems = (List<Map<String, Object>>) ((List) listOfTrafficVolumes).get(0);
                    changeTime = last(trafficVolumeItems, "changeTime");
                    if (changeTime != null) {
                        Date eventEndTime; // EVENT_END_TIME;
                        try {
                            eventEndTime = sdfS.get().parse(changeTime.toString());
                            BigDecimal duration;
                            if (eventEndTime != null) {
                                record.put("EVENT_END_TIME", sdfT.get().format(eventEndTime));
                                duration = new BigDecimal((eventEndTime.getTime() - eventStartTime.getTime()) / 1000);
                                record.put("ORIGINAL_DUR", duration.setScale(0, BigDecimal.ROUND_CEILING));
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                        Long dataVolumeGPRSUplink = sum(trafficVolumeItems, "dataVolumeGPRSUplink");
                        record.put("datavolumeGPRSUplink", dataVolumeGPRSUplink);

                        Long dataVolumeGPRSDownlink = sum(trafficVolumeItems, "dataVolumeGPRSDownlink");
                        record.put("datavolumeGPRSDownlink", dataVolumeGPRSDownlink);

                        Long dataVolumeGPRS = dataVolumeGPRSUplink + dataVolumeGPRSDownlink;
                        record.put("dataVolumeGPRS", dataVolumeGPRS);

                        float BILLABLE_VOLUME = dataVolumeGPRS / 1024;
                        record.put("BILLABLE_VOLUME", ((Double) Math.ceil(BILLABLE_VOLUME)).intValue());

                        String ZERO_BYTE_IND = "0";
                        if (dataVolumeGPRS == 0) {
                            ZERO_BYTE_IND = "1";
                        }
                        record.put("ZERO_BYTE_IND", ZERO_BYTE_IND);
                    }
                }
            }

            Object ipAddress1 = record.get("sgsnAddress");
            if (ipAddress1 instanceof List && !((List) ipAddress1).isEmpty()){
                record.put("SGSN_ADDRESS", ((List) ipAddress1).get(0));
            }

            Object ipAddress2 = record.get("ggsnAddress");
            if (ipAddress2 instanceof List && !((List) ipAddress2).isEmpty()){
                record.put("GGSN_ADDRESS", ((List) ipAddress2).get(0));
            }

            Object ipAddress3 = record.get("servedPDPAddress");
            if (ipAddress3 instanceof List && !((List) ipAddress3).isEmpty()){
                record.put("PDP_ADDRESS", ((List) ipAddress3).get(0));
            }

            if (record.containsKey("changeOfServiceCondition")){
                logger.warn("SGSN key `changeOfServiceCondition` found. Event - {}", record.toString());
            }
//            try {
//                List<Map<String, Object>> coscs = (List<Map<String, Object>>) record.get("changeOfServiceCondition");
//                if (coscs != null && coscs.size() > 0) {
//                    changeTime = last(coscs, "timeOfLastUsage");
//                }
//                if (coscs != null) {
//                    record.put("ratingGroup", dsv(coscs, "ratingGroup"));
//                    record.put("chargingRuleBaseName", dsv(coscs, "chargingRuleBaseName"));
//                    record.put("resultCode", dsv(coscs, "resultCode"));
//                    record.put("localSequenceNumber", dsv(coscs, "localSequenceNumber"));
//                    record.put("timeOfFirstUsage", first(coscs, "timeOfFirstUsage"));
//                    record.put("timeOfLastUsage", last(coscs, "timeOfLastUsage"));
//                    record.put("timeUsage", sum(coscs, "timeUsage"));
//                    record.put("serviceConditionChange", dsv(coscs, "serviceConditionChange"));
//
//                    record.put("sgsnAddress", distinctValueAsDSV(coscs, "sgsnAddress"));
//                    record.put("timeOfReport", last(coscs, "timeOfReport"));
//                    record.put("userLocationInformation", dsv(coscs, "userLocationInformation"));
//
//                    Long datavolumeFBCUplink = sum(coscs, "datavolumeFBCUplink");
//                    record.put("datavolumeFBCUplink", datavolumeFBCUplink);
//
//                    Long datavolumeFBCDownlink = sum(coscs, "datavolumeFBCDownlink");
//                    record.put("datavolumeFBCDownlink", datavolumeFBCDownlink);
//
//                    Long datavolumeFBC = datavolumeFBCUplink + datavolumeFBCDownlink;
//                    record.put("datavolumeFBC", datavolumeFBC);
//                }
//            } catch (Exception e) {
//                logger.error(e.getMessage(), e);
//            }

            record.put("SRV_TYPE_KEY", "3");

            String edrIdKey = EVENT_TYPE_KEY + '|' + servedMSISDN + '|'
                    + accessPointNameOI.toString() + "|"
                    + EVENT_START_TIME + '|'
                    + String.valueOf(record.get("datavolumeGPRSUplink")) + "|"
                    + String.valueOf(record.get("datavolumeGPRSDownlink")) + "|"
                    + REC_TYPE_ID_KEY;
            record.put("EDR_ID_KEY", edrIdKey);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(record);
        return txRecord;
    }

    String dsv(List<Map<String, Object>> group, String key) {
        String c = group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString())
                .reduce("", (x, y) -> x + y + "|");
        if (c != null && c.length() > 1) {
            c = c.substring(0, c.length() - 1);
        }
        return c;
    }

    Long sum(List<Map<String, Object>> group, String key) {
        return group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> Long.parseLong(e.get(key).toString()))
                .reduce(0l, (a, b) -> a + b);
    }

    Object first(List<Map<String, Object>> group, String key) {
        List<Object> l = group.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    Object last(List<Map<String, Object>> group, String key) {
        List<Object> l = group.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }

    String distinctValueAsDSV(List<Map<String, Object>> coscs, String key) {
        List<Map<String, Object>> list = coscs.stream().filter(distinct(p -> p.get(key))).collect(Collectors.toList());
        return dsv(list, key);
    }

    private static <T> Predicate<T> distinct(Function<? super T, Object> key) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(key.apply(t), Boolean.TRUE) == null;
    }

    private String ltrim(String s, char c) {
        int len = s.length();
        int st = 0;
        char[] val = s.toCharArray();
        while ((st < len) && (val[st] == 'c')) {
            st++;
        }
        return ((st > 0) || (len < s.length())) ? s.substring(st, len) : s;
    }

}

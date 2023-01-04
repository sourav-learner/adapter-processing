package com.gamma.skybase.build.server.etl.tx.gsn;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by abhi on 3/6/2017
 */


public class GGSNRecordEnrichmentOld implements IEnrichment {

    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private static final Logger logger = LoggerFactory.getLogger(GGSNRecordEnrichmentOld.class);
    static DecimalFormat df = new DecimalFormat("#.##");

    private static final String FORMAT1 = "yyMMddHHmmss";
    private static final String FORMAT2 = "yyyyMMdd HH:mm:ss";

    private static final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT1));
    private static final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT2));

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
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(record);
        try {
            //"pGWAddress" -> " size = 1"
            //servingNodeAddress
            //servedPDPPDNAddress


            txRecord.put("pGWAddress", extractIpAddress(txRecord.get("pGWAddress")));
            txRecord.put("servingNodeAddress", extractIpAddress(txRecord.get("servingNodeAddress")));
            txRecord.put("servedPDPPDNAddress", extractIpAddress(txRecord.get("servedPDPPDNAddress")));


            String EVENT_TYPE_KEY = "4";
            Object accessPointNameNI = txRecord.get("accessPointNameNI");
            if (accessPointNameNI != null) {
//                EVENT_TYPE_KEY = transformationLib.getDimLookup(accessPointNameNI.toString().trim(), "19");
                if (accessPointNameNI.toString().equalsIgnoreCase("mms")) EVENT_TYPE_KEY = "3";
            }
            txRecord.put("EVENT_TYPE_KEY", EVENT_TYPE_KEY);

            String REC_TYPE_ID_KEY;
            Object recordSequenceNumber = txRecord.get("recordSequenceNumber");
            if (recordSequenceNumber == null)
                REC_TYPE_ID_KEY = "4";
            else if (recordSequenceNumber.toString().equals("1"))
                REC_TYPE_ID_KEY = "5";
            else {
                Object causeForRecClosing = txRecord.get("causeForRecClosing");
                if (causeForRecClosing.toString().equals("0") || causeForRecClosing.toString().equals("1"))
                    REC_TYPE_ID_KEY = "7";
                else
                    REC_TYPE_ID_KEY = "6";
            }
            txRecord.put("REC_TYPE_ID_KEY", REC_TYPE_ID_KEY);

            String SRV_TYPE_KEY = null;
            Object servingNodePLMNIdentifier = txRecord.get("servingNodePLMNIdentifier");
            Object userLocInfo = txRecord.get("userLocationInformation");
            Object servedIMSI = txRecord.get("servedIMSI");
            if (servedIMSI != null) {
                if("634012002350357".equalsIgnoreCase(servedIMSI.toString())) {
                    logger.debug("debug");
                }
            }

            Object chargingCharacteristics = txRecord.get("chargingCharacteristics");
            if (chargingCharacteristics != null) {
                chargingCharacteristics = ltrim(chargingCharacteristics.toString(), '0').trim();
                boolean hasPLMN = servingNodePLMNIdentifier != null && servingNodePLMNIdentifier.toString().startsWith("63401");
                switch (chargingCharacteristics.toString().trim()) {
                    case "4":
                        if (hasPLMN)
                            SRV_TYPE_KEY = "1";
                        else
                            SRV_TYPE_KEY = "5";
                        break;

                    case "8":
                        if (hasPLMN)
                            SRV_TYPE_KEY = "2";
                        else
                            SRV_TYPE_KEY = "6";
                        break;
                    default:
                        SRV_TYPE_KEY = "-99";
//                            SRV_TYPE_KEY = transformationLib.getDimLookup(chargingCharacteristics.toString(), "15");
                        break;
                }
            }


            Object servedMSISDN = txRecord.get("servedMSISDN");
            boolean hasServedIMSI = servedIMSI != null && !servedIMSI.toString().trim().isEmpty();
            boolean hasServedMSISDN = servedMSISDN != null && !servedMSISDN.toString().trim().isEmpty();
            if (!(hasServedIMSI || hasServedMSISDN))
                SRV_TYPE_KEY = "-90";
            txRecord.put("SRV_TYPE_KEY", SRV_TYPE_KEY);


            // EVENT_START_TIME;
            Object recordOpeningTime = txRecord.get("recordOpeningTime");
            if (recordOpeningTime != null) {
                try {
                    Date f = sdfS.get().parse(recordOpeningTime.toString());
                    txRecord.put("recordOpeningTime", sdfT.get().format(f));
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }

//            List<Map<String, Object>> coccs = (List<Map<String, Object>>) txRecord.get("changeOfCharCondition");
            List<Object> listOfTrafficVolumes = (List<Object>) txRecord.get("listOfTrafficVolumes");
            List<Map<String, Object>> coccs = flattenListOfData(listOfTrafficVolumes);
//            List<Map<String, Object>> coscs = (List<Map<String, Object>>) txRecord.get("changeOfServiceCondition");
            List<Object> listOfServiceData = (List<Object>) txRecord.get("listOfServiceData");
            List<Map<String, Object>> coscs = flattenListOfData(listOfServiceData);
            String EVENT_START_TIME = null;
            Date eventStartTime = null;
            try {
                if (recordOpeningTime == null)
                    recordOpeningTime = first(coscs, "timeOfFirstUsage");
                eventStartTime = sdfS.get().parse(recordOpeningTime.toString());
                EVENT_START_TIME = sdfT.get().format(eventStartTime);
                txRecord.put("EVENT_START_TIME", EVENT_START_TIME); // 2
                txRecord.put("XDR_DATE", EVENT_START_TIME); // 2
                txRecord.put("POPULATION_DATE_TIME", sdfT.get().format(new Date())); // 2
                txRecord.put("GENERATED_FULL_DATE", new SimpleDateFormat("yyyyMMdd").format(eventStartTime) + " 00:00:00"); //31
                txRecord.put("EVENT_START_TIME_SLOT_KEY", new SimpleDateFormat("yyyyMMdd HH").format(eventStartTime) + ":00:00");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            Date currentTime = new Date();
            txRecord.put("POPULATION_DATE_TIME", sdfT.get().format(currentTime));

            Object changeTime = null;
            if (coccs != null && coccs.size() > 0)
                changeTime = last(coccs, "changeTime");

            if (coscs != null && coscs.size() > 0)
                changeTime = last(coscs, "timeOfLastUsage");

            Date eventEndTime = null; // EVENT_END_TIME;
            if (changeTime != null) {
                try {
                    eventEndTime = sdfS.get().parse(changeTime.toString());
                    txRecord.put("EVENT_END_TIME", sdfT.get().format(eventEndTime));
                    BigDecimal duration;
                    if (eventStartTime != null) {
                        duration = new BigDecimal((eventEndTime.getTime() - eventStartTime.getTime()) / 1000);
                        txRecord.put("ORIGINAL_DUR", duration.setScale(0, BigDecimal.ROUND_CEILING));
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }

            txRecord.put("ORIGINAL_A_NUM", servedMSISDN);
            if (servedMSISDN != null && !servedMSISDN.equals("")) {
                servedMSISDN = servedMSISDN.toString().substring(2);
                txRecord.put("servedMSISDN", servedMSISDN);
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN.toString());
                if (ddk != null) {
                    txRecord.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    txRecord.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                }
            }

            Long dataVolumeGPRSUplink = sum(coccs, "dataVolumeGPRSUplink");
            txRecord.put("dataVolumeGPRSUplink", dataVolumeGPRSUplink);

            Long dataVolumeGPRSDownlink = sum(coccs, "dataVolumeGPRSDownlink");
            txRecord.put("dataVolumeGPRSDownlink", dataVolumeGPRSDownlink);

            float dataVolumeGPRS = dataVolumeGPRSUplink + dataVolumeGPRSDownlink;
            if(dataVolumeGPRS == 0) return null;
            txRecord.put("dataVolumeGPRS", df.format(dataVolumeGPRS));

            float BILLABLE_VOLUME = dataVolumeGPRS / 1024;
            txRecord.put("BILLABLE_VOLUME", ((Double) Math.ceil(BILLABLE_VOLUME)).intValue());

            String ZERO_BYTE_IND = "0";
            if (dataVolumeGPRS == 0)
                ZERO_BYTE_IND = "1";
            txRecord.put("ZERO_BYTE_IND", ZERO_BYTE_IND);

            try {
                if (coscs != null) {
                    List<Object> list = coscs.stream()
                            .filter(e -> e.containsKey("ratingGroup"))
                            .filter(distinct(p -> p.get("ratingGroup")))
                            .map(p -> p.get("ratingGroup"))
                            .collect(Collectors.toList());


                    txRecord.put("ratingGroup", distinctValueAsDSV(coscs, "ratingGroup"));
                    List<Map<String, Object>> groupByRatingGroup = new ArrayList<>();
                    for (Object element : list) {
                        List<Map<String, Object>> group = coscs.stream()
                                .filter(e -> e.containsKey("ratingGroup"))
                                .filter(p -> p.get("ratingGroup").equals(element))
                                .collect(Collectors.toList());
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("timeUsage", Long.valueOf(sum(group, "timeUsage")));

                        Long dataVolumeFBCUplink = sum(group, "dataVolumeFBCUplink");
                        m.put("dataVolumeFBCUplink", dataVolumeFBCUplink);

                        Long dataVolumeFBCDownlink = sum(group, "dataVolumeFBCDownlink");
                        m.put("dataVolumeFBCDownlink", dataVolumeFBCDownlink);

                        double dataVolumeFBC = (double) (dataVolumeFBCUplink + dataVolumeFBCDownlink)/1024;
                        m.put("dataVolumeFBC", df.format(Math.ceil(dataVolumeFBC)));

                        groupByRatingGroup.add(m);
                    }

                    txRecord.put("timeUsage", dsv(groupByRatingGroup, "timeUsage"));
                    txRecord.put("dataVolumeFBCUplink", dsv(groupByRatingGroup, "dataVolumeFBCUplink"));
                    txRecord.put("dataVolumeFBCDownlink", dsv(groupByRatingGroup, "dataVolumeFBCDownlink"));
                    txRecord.put("dataVolumeFBC", dsv(groupByRatingGroup, "dataVolumeFBC"));


                    txRecord.put("chargingRuleBaseName", distinctValueAsDSV(coscs, "chargingRuleBaseName"));
                    txRecord.put("resultCode", distinctValueAsDSV(coscs, "resultCode"));
                    txRecord.put("localSequenceNumber", distinctValueAsDSV(coscs, "localSequenceNumber"));

                    if (EVENT_START_TIME != null)
                        txRecord.put("timeOfFirstUsage", EVENT_START_TIME);
                    if (eventStartTime != null)
                        txRecord.put("timeOfLastUsage", sdfT.get().format(eventEndTime));

                    txRecord.put("serviceConditionChange", distinctValueAsDSV(coscs, "serviceConditionChange"));
                    txRecord.put("sgsnAddress", getSgsnAddress(coscs, "sgsnAddress"));

                    Object tor = last(coscs, "timeOfReport");
                    if (tor != null && !tor.toString().isEmpty()) {
                        try {
                            Date timeOfReport = sdfS.get().parse(tor.toString());
                            txRecord.put("timeOfReport", sdfT.get().format(timeOfReport));
                        } catch (ParseException e){
                            logger.error(e.getMessage(), e);
                        }
                    }
                    txRecord.put("userLocationInformation", distinctValueAsDSV(coscs, "userLocationInformation"));
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            String edrIdKey = EVENT_TYPE_KEY + '|' + servedMSISDN + '|' + EVENT_START_TIME + '|'
                    + BILLABLE_VOLUME
                    + '|' + dataVolumeGPRSUplink + '|' + dataVolumeGPRSDownlink + '|' + REC_TYPE_ID_KEY;
            txRecord.put("EDR_ID_KEY", edrIdKey);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return txRecord;
    }

    private Object getSgsnAddress(List<Map<String, Object>> coscs, String keyName) {
        Set<String> ips = new HashSet<>();
        if(coscs != null){
            for(Map<String, Object> map: coscs){
                Object value = map.get(keyName);
                if(value != null){
                    ips.add(extractIpAddress(value));
                }
            }
        }
        return StringUtils.join(ips, "|");
    }

    private List<Map<String, Object>> flattenListOfData(List<Object> listOfServiceData) {
        List<Map<String, Object>> dataset = new ArrayList<>();
        if(listOfServiceData!= null) {
            for (Object obj : listOfServiceData) {
                if (obj instanceof List) {
                    for (Object o : (List<Object>) obj) {
                        Map<String, Object> data = (Map<String, Object>) o;
                        dataset.add(data);
                    }
                } else {
                    Map<String, Object> data = (Map<String, Object>) obj;
                    dataset.add(data);
                }
            }
        }
        return dataset;
    }

    private String extractIpAddress(Object addressList) {
        String extract;
        if(addressList instanceof List){
            List<String> ips = (ArrayList<String>) addressList;
            extract = StringUtils.join(ips, "|");
        } else {
            extract = String.valueOf(addressList);
        }
        return extract;
    }

    static String dsv(List<Map<String, Object>> group, String key) {
        String c = group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString())
                .reduce("", (x, y) -> x + y + "|");
        if (c != null && c.length() > 1) {
            c = c.substring(0, c.length() - 1);
        }
        return c;
    }

    static Long sum(List<Map<String, Object>> group, String key) {
        return group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> Long.parseLong(e.get(key).toString()))
                .reduce(0l, (a, b) -> a + b);
    }

    static Object first(List<Map<String, Object>> group, String key) {
        List<Object> l = group.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    static Object last(List<Map<String, Object>> group, String key) {
        List<Object> l = group.stream().map(e -> e.get(key))
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }

    static String distinctValueAsDSV(List<Map<String, Object>> coscs, String key) {
        List<Map<String, Object>> list = coscs.stream()
                .filter(e -> e.containsKey(key))
                .filter(distinct(p -> p.get(key))).collect(Collectors.toList());
        return dsv(list, key);
    }

    private static <T> Predicate<T> distinct(Function<? super T, Object> key) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(key.apply(t), Boolean.TRUE) == null;
    }

    String normalize(String number) {
        if (number == null) {
            return "0";
        }
        if (number.length() > 2) {
            if (number.startsWith("11") || number.startsWith("13")) {
                return ltrim(number.substring(2), '0');
            }
            if (number.startsWith("12") || number.startsWith("14")) {
                String t = number.substring(2);
                if (t.startsWith("00")) {
                    return t.substring(1);
                }
                if (t.startsWith("0")) {
                    t = t.substring(1);
                }
                return "249" + t;
            }
        }
        if (!NumberUtils.isNumber(number)) {
            return ltrim(number, '0');
        }

//        if (number.startsWith(ton + np)) number = number.substring(2);
        if (!NumberUtils.isNumber(number)) {
            number = number.substring(3);
            number = ltrim(number, '0');
        }
        return number;
    }

    private String ltrim(String s, char c) {
        int len = s.length();
        int st = 0;
        char[] val = s.toCharArray();
        while ((st < len) && (val[st] == c)) st++;
//      while ((st < len) && (val[len - 1] <= 'c')) len--;
        return ((st > 0) || (len < s.length())) ? s.substring(st, len).trim() : s.trim();
    }

}


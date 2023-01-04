package com.gamma.skybase.build.server.etl.utils;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.StringUtility;
import com.gamma.telco.opco.ReferenceDimServiceRangeLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by abhi on 2019-04-24
 */
public class OpcoBusinessTransformation extends com.gamma.telco.OpcoBusinessTransformation {

    private static final Logger logger  = LoggerFactory.getLogger(OpcoBusinessTransformation.class);
    private static Map<String, Object> AIR_META_CONTAINER = new ConcurrentHashMap<>();

    private Map<String, String> sdrValues = new ConcurrentHashMap<>();

    /*
     * 1 signifies manual adjustments
     * 2 signifies me2u adjustments
     * 3 signifies subscription adjustments
     * */
    public String getAirAdjustmentRecordType(String originNodeType, String originHost, String otherParty) {

        if (AIR_META_CONTAINER.isEmpty()) loadAirMetaContainer();
        String key = originNodeType.trim() + "#" + "AdjustmentRecordV2" + "#" + originHost.trim();


        if (AIR_META_CONTAINER.containsKey(key.toLowerCase())) {
            ReferenceDimAirService reference = (ReferenceDimAirService) AIR_META_CONTAINER.get(key.toLowerCase());
            String recordType = reference.getServiceIdKey();
            if (recordType != null) {
                if (otherParty != null) {
//                    if (otherParty.trim().matches("^.*\\d$")) {
                    if (otherParty.trim().toLowerCase().contains("faf") || otherParty.trim().contains("sfd")) {
                        recordType = "3";
                    }
                }
            }
            return recordType;
        } else {
            String msg = "Error from getAirAdjustmentRecordType for input originNodeType == " + originNodeType +
                    ", otherParty == " + otherParty + ", originHost == " + originHost + ", key == " + key;
            logger.info(msg);
            return null;
        }
    }

    private static synchronized void loadAirMetaContainer() {

//        AIR_META_CONTAINER.clear();
        Map<Object, Object> items = cache.getCacheTuples("DIM_AIR_SERVICE_CACHE");
        for (Map.Entry<Object, Object> entry : items.entrySet()) {
            AIR_META_CONTAINER.put(String.valueOf(entry.getKey()).toLowerCase(), entry.getValue());
        }
    }

    /*
     * Types of subscription key
     * 1 denotes FAF
     * 2 denotes sfd
     * 3 denotes black & green
     * 4 denotes offer voice
     * 5 denotes offer sms
     * 6 denotes offer gprs
     * 7 denotes bundle
     * */
    public Map<String, String> getAirSubscriptionRecKey(String originHost, String otherNumber, String transactionCode) {
        Map<String, String> subscriptionProperties = new HashMap<>();
        String subscriptionRecKey = "", serviceName = "";
        if ("USSDGWIAXF".equalsIgnoreCase(originHost != null ? originHost.trim() : originHost)) {
            if (otherNumber.trim().toLowerCase().contains("faf")) {
                subscriptionRecKey = "1";
                serviceName = "FAF";
            } else if (otherNumber.trim().toLowerCase().contains("sfd")) {
                subscriptionRecKey = "2";
                serviceName = "SFD";
            }
        } else if (StringUtility.arrayContainsIgnoreCase(new String[]{"MagicVoice", "Islamic", "Bible", "GamingPortal"}, originHost.trim())) {
            subscriptionRecKey = "3";
            serviceName = "Black & Green";
        } else {
            if (transactionCode.trim().startsWith("DOV")) {
                subscriptionRecKey = "4";
                serviceName = "Offer Voice";
            } else if (transactionCode.trim().startsWith("DOS")) {
                subscriptionRecKey = "5";
                serviceName = "Offer SMS";
            } else if (transactionCode.trim().startsWith("DOG")) {
                subscriptionRecKey = "6";
                serviceName = "Offer GPRS";
            } else if (transactionCode.trim().startsWith("DOB")) {
                subscriptionRecKey = "7";
                serviceName = "Bundle";
            }
        }


        subscriptionProperties.put("subscriptionRecKey",subscriptionRecKey);
        subscriptionProperties.put("serviceName",serviceName);
        return subscriptionProperties;
    }

    public Object getPostPreLookupBasedOnServiceClass(String serviceClassId, String servedMsisdn) {

        return "pre";
    }

    /**
     * Origin Node Type value can be of below types (EXT, ADM, UGW)
     * EXT -- Denotes eRecharge
     * ADM (MINSAT Customer care application) -- Denotes Subscription Offer, which is categorized as one of voucher recharge
     * UGW (USSD gateway) -- Denotes voucher based recharge
     * For voucher based Recharge recordType value will be 1
     * For eRecharge recordType value will be 2
     * For ADM (subscription offer) recordType value will be 3
     *
     * @param originNodeType
     * @param originHost
     * @return
     */
    public String getAirRefillRecordType(String originNodeType, String originHost) {

        if (AIR_META_CONTAINER.isEmpty()) loadAirMetaContainer();
        String key = originNodeType.trim() + "#" + "RefillRecordV2" + "#" + originHost.trim();

        if (AIR_META_CONTAINER.containsKey(key.toLowerCase())) {
            ReferenceDimAirService reference = (ReferenceDimAirService) AIR_META_CONTAINER.get(key.toLowerCase());
            return reference.getServiceIdKey();
        } else {
            String msg = "Error from getAirRefillRecordType for input originNodeType == " + originNodeType +
                    ", originHost == " + originHost + ", key == " + key;
            logger.info(msg);
            return null;
        }
    }

    public String getSdrValue(Date cdrDate) {
        if (sdrValues.isEmpty()) {
            Map<Object, Object> cacheEntries = cache.getCacheTuples("DIM_SERVICE_RANGE_LOOKUP_CACHE");
            if (cacheEntries != null) {
                for (Object obj : cacheEntries.values()) {
                    if (obj instanceof ReferenceDimServiceRangeLookup) {
                        ReferenceDimServiceRangeLookup lookup = (ReferenceDimServiceRangeLookup) obj;
                        if ("SDR_RATE".equalsIgnoreCase(lookup.getLookupKey())) {
                            Date start = lookup.getValidFromUtilJavaDate();
                            if (start == null) start = DateUtility.subtractDays(new Date(), 2*365);
                            Date end = lookup.getValidToFromUtilJavaDate();
                            if (end == null) end = DateUtility.addDays(new Date(), 90);
                            List<String> dateList = DateUtility.getDatesBetweenRange(start, end);
                            dateList.forEach(date -> sdrValues.put(date, lookup.getLookupOutput()));
                        }
                    }
                }
            }
        }

        String narrowDate = DateUtility.convertJavaUtil2NarrowStringDate(cdrDate);
        return sdrValues.get(narrowDate);
    }
}

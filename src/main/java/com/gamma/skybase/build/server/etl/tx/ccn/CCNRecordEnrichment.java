package com.gamma.skybase.build.server.etl.tx.ccn;


import com.gamma.components.commons.StringUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.utility.IntlZoneConfigManager;
import com.gamma.skybase.build.utility.MIntlZone;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
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
@SuppressWarnings("Duplicates")
public class CCNRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(CCNRecordEnrichment.class);

    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private final AppConfig appConfig = AppConfig.instance();
    private final CCNRecordUtility recordUtility = new CCNRecordUtility();
    private final String offset = appConfig.getProperty("app.datasource.ccn.timeoffset");

    private DedicatedAccountsHandler dedicatedAccountsHandler;
    private AccumulatorsHandler accumulatorsHandler;
    private CCAccountDataHandler ccAccountDataHandler;

    private static final String FORMAT1 = "yyyyMMdd HH:mm:ss";
    private static final String FORMAT2 = "yyyy-MM-dd HH:mm:ss";
    private static final String FORMAT3 = "yyyyMMdd";

    private static final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT1));
    private static final ThreadLocal<SimpleDateFormat> sdfT2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT2));
    private static final ThreadLocal<SimpleDateFormat> yyyymmdd = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT3));

    DecimalFormat df = new DecimalFormat("#.000000");

    private final Date currentTime = new Date();
    private final String countryCode = appConfig.getProperty("app.datasource.countrycode");//North sudan

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
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> r) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();
        //TODO to be removed
//        if(!r.get("serviceContextID").toString().toLowerCase().equals("scap_v.2.0@ericsson.com")) return record;

        dedicatedAccountsHandler = new DedicatedAccountsHandler();
        accumulatorsHandler = new AccumulatorsHandler();
        ccAccountDataHandler = new CCAccountDataHandler();

        String partialSequenceNumber = null;
        ArrayList<LinkedHashMap<String, Object>> creditControlRecord = new ArrayList<>();
        for (Map.Entry<String, Object> entry : r.entrySet()) {
            String key = entry.getKey();
            String serviceSessionID;
            ArrayList<LinkedHashMap<String, Object>> correlationID;
            switch (key) {
                case "triggerTime":
                    record.put("olccTriggerTime", entry.getValue().toString());
                    break;

                case "nodeName":// Rule 8 NE_ID_KEY --get it from lookup nodeName
                    record.put(key, entry.getValue().toString());
                    String neIdKey = transformationLib.getNEIdKey(entry.getValue().toString().trim());
                    if (neIdKey != null) {
                        record.put("NE_ID_KEY", neIdKey);
                    }
                    break;

                case "serviceSessionID":
                    serviceSessionID = entry.getValue().toString();
                    record.put("serviceSessionID", serviceSessionID);
                    break;

                case "recordIdentificationNumber":
                    record.put(key, entry.getValue().toString());
                    break;

                case "partialSequenceNumber": // Rule  11	REC_TYPE_ID_KEY
                    partialSequenceNumber = entry.getValue().toString();
                    record.put(key, partialSequenceNumber);
                    if (partialSequenceNumber == null) {
                        record.put("REC_TYPE_ID_KEY", "4");
                    } else {
                        record.put("REC_TYPE_ID_KEY", "3");
                    }
                    break;

                case "servedSubscriptionID":
                    Map<String, String> servedSubscriptionIDs = traverseServedSubscription(entry.getValue());
                    record.putAll(servedSubscriptionIDs);
                    break;

                case "correlationID":
                    correlationID = (ArrayList<LinkedHashMap<String, Object>>) entry.getValue();
                    Map<String, String> correlations = traverseCorrelationId(correlationID);
                    record.putAll(correlations);
                    break;

                case "servingElement":
                    ArrayList<LinkedHashMap<String, Object>> servingElement = (ArrayList<LinkedHashMap<String, Object>>) entry.getValue();
                    Map<String, String> servingElements = traverseServingElement(servingElement);
                    record.putAll(servingElements);
                    break;

                case "creditControlRecord":
                    creditControlRecord = (ArrayList<LinkedHashMap<String, Object>>) entry.getValue();
                    break;

                default:
                    record.put(key, entry.getValue().toString());
            }
        }

        Map<String, Object> ccr = null;
        if (creditControlRecord != null) {
            try {
                ccr = flattenCreditControlRecords(creditControlRecord);
                record.putAll(ccr);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        LinkedHashMap<String, Object> chargingContextSpecific = (LinkedHashMap<String, Object>) ccr.get("chargingContextSpecific");

        Object bae = ccr.get("accountValueAfter");
        if (bae != null && !bae.toString().trim().isEmpty()) {
            try {
                record.put("BAL_AFTER_EVENT", df.format(Double.parseDouble(bae.toString()) / 1000000));
            } catch (Exception e) {//
            }
        }
        Object bbe = ccr.get("accountValueBefore");
        if (bbe != null && !bbe.toString().trim().isEmpty()) {
            try {
                record.put("BAL_BEFORE_EVENT", df.format(Double.parseDouble(bbe.toString()) / 1000000));
            } catch (Exception e) {//
            }
        }

        Double charge = 0D;
        Object avd = ccr.get("accountValueDeducted");
        if (avd != null && !avd.toString().trim().isEmpty()) {
            try {
                charge = Double.parseDouble(avd.toString()) / 1000000;
            } catch (Exception e) {//
            }
        }
        record.put("CHARGE", df.format(charge));

        Object servedAccount = ccr.get("servedAccount");
        if (servedAccount != null)
            record.put("servedAccount", ccr.get("servedAccount").toString());

        Object serviceClassID = ccr.get("serviceClassID");
        if (serviceClassID != null)
            record.put("serviceClassID", ccr.get("serviceClassID").toString());

        Object accountGroupID = ccr.get("accountGroupID");
        if (accountGroupID != null)
            record.put("accountGroupID", ccr.get("accountGroupID").toString());

        Object serviceOfferings = ccr.get("serviceOfferings");
        if (serviceOfferings != null)
            record.put("serviceOfferings", ccr.get("serviceOfferings").toString());

        String sysIdKey = appConfig.getProperty("app.datasource.ccn.sysidkey");
        record.put("SYS_ID_KEY", sysIdKey);



        String subType = "PRE";
        if(serviceClassID == null) return null;
        Integer sid = Integer.parseInt(serviceClassID.toString());
        if(sid >= 700) subType = "POST";
//        if (serviceClassID.toString().compareTo("700") >= 0) subType = "POST";
        String ccid = record.get("serviceContextID").toString().toLowerCase();
        String recType = transformationLib.getEventType(ccid);

        if (recType == null) {
            logger.info("unknown serviceContextID encountered  -> "+ ccid);
            recType = "UNK";
        } else {
            recType = recType + '_' + subType;
        }
        record.put("_EVENT_TYPE_", recType);

        /* backlog changes commented */
//        if (!"SCAP_PRE".equalsIgnoreCase(recType)){ //TODO for SCAP Prepaid backlog processing
//            return null;
//        }


        Object serviceScenario = ccr.get("serviceScenario");
        Object roamingPosition = ccr.get("roamingPosition");
        Object serviceIdentifier = ccr.get("serviceIdentifier");
        if (serviceScenario == null) {
            serviceScenario = "";
        }
        if (roamingPosition == null) {
            roamingPosition = "";
        }
        if (serviceIdentifier == null) {
            serviceIdentifier = "";
        }
        String eventTypeKey = transformationLib.getCCNEventTypeKey(
                serviceIdentifier.toString(), serviceScenario.toString(),
                roamingPosition.toString(), sysIdKey);

        if(recType.equalsIgnoreCase("gy_pre") || recType.equalsIgnoreCase("gy_post")){
            if(eventTypeKey == null && !serviceIdentifier.toString().equals("41")){
                eventTypeKey = "4"; //GPRS Data, MMS will come from lookup
            }
        }

        if(recType.equalsIgnoreCase("sip_pre") || recType.equalsIgnoreCase("sip_post")) {
            eventTypeKey = "19"; //SIP calls
        }

        if (eventTypeKey == null) {
            eventTypeKey = "-99";
        }

        record.put("EVENT_TYPE_KEY", eventTypeKey); // Voice,Transit,RCF,CF, SMS, MMS, GPRS

        Object totalOctetsUnit = ccr.get("totalOctetsUnit");
        if (totalOctetsUnit != null)
            record.put("ORIGINAL_VOLUME", totalOctetsUnit);

        Date triggerTime = new Date(0);
        Object ccrTriggerTime = ccr.get("triggerTime");
        Object olccTriggerTime = record.get("olccTriggerTime");
        Object ccrEventTime = ccr.get("eventTime");
        Long timeUnit = ccr.get("timeUnit") == null ? 0L : Long.parseLong(ccr.get("timeUnit").toString());
        Long servSpecUnit = ccr.get("servSpecUnit") == null ? 0L : Long.parseLong(ccr.get("servSpecUnit").toString());

        Map<String, Object> timeUsages = recordUtility.getTimeUsageValues(olccTriggerTime, ccrTriggerTime,
                ccrEventTime, timeUnit, servSpecUnit);

        if(timeUsages == null) return null;
        else record.putAll(timeUsages);

        try {
            triggerTime = sdfT.get().parse(record.get("EVENT_START_TIME").toString());
        } catch (ParseException e) {
            logger.debug(e.getMessage());
        }
        Long origDur = (Long) record.get("ORIGINAL_DUR");

        Double billableVolume = 0d;
        int serviceIdentifierVal;
        if (serviceIdentifier == "") {
            serviceIdentifierVal = 0;
        } else {
            serviceIdentifierVal = Integer.parseInt(serviceIdentifier.toString());
        }

        int billablePulse = 0;

        Object callReference = record.get("callReference") == null ? "" : record.get("callReference");
        Object sessionIdUTF = record.get("sessionIdUTF");
        Object serviceSessionID = record.get("serviceSessionID");
        Object ctx16778226 = chargingContextSpecific.get("16778226");

        String serviceSession = null;
        if (ctx16778226 != null && !ctx16778226.toString().trim().isEmpty())
            serviceSession = ctx16778226.toString().trim();
        else if (sessionIdUTF != null && !sessionIdUTF.toString().trim().isEmpty())
            serviceSession = sessionIdUTF.toString().trim();
        else if (serviceSessionID != null && !serviceSessionID.toString().trim().isEmpty())
            serviceSession = serviceSessionID.toString().trim();

        Object ctx16777466 = chargingContextSpecific.get("16777466");

        Object orgBNum = null;
        switch (recType.toUpperCase()) {
            case "SCF_PRE":
            case "SCF_POST":
                orgBNum = chargingContextSpecific.get("16778219");
                switch (serviceIdentifierVal) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 7:
                    case 8:
                        orgBNum = chargingContextSpecific.get("16778219");//?
                        break;
                    default:
                        logger.debug("Inspect File "+record.get("fileName") +" for serviceIdentifier "+serviceIdentifierVal);
                        break;
                }
                if (serviceIdentifierVal < 65526 && serviceIdentifierVal > 65535) {
                    billablePulse = 1;
                } else if(servSpecUnit > 0){
                    billablePulse = servSpecUnit.intValue();
                } else {
                    try {
                        billablePulse = (int) Math.ceil((double) origDur / 60);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                record.put("ZERO_DURATION_IND", origDur == 0 ? "1" : "0");
                Object ctx16777417 = chargingContextSpecific.get("16777417"); //SPI_UTF_PARAM
                Double d = 0D;
                if (ctx16777417 != null && !ctx16777417.toString().trim().isEmpty()) {
                    try {
                        d = Double.parseDouble(ctx16777417.toString().trim());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                record.put("FLEXI_COL_1", d);
                if (callReference != null) record.put("EVENT_REF_NUM", callReference.toString());
                break;

            case "SCAP_PRE":
            case "SCAP_POST":
                record.put("ZERO_DURATION_IND", "0");
                billablePulse = 1;

                switch (serviceIdentifierVal) {
                    case 0:
                    case 99:
                    case 10:
                    case 65532:
                    case 65535:
                        orgBNum = chargingContextSpecific.get("16778219");
                        break;
                    case 65534:
                        orgBNum = chargingContextSpecific.get("16777418");
                        break;
                    default://11,12,61,62,65530,65531,65533
                        orgBNum = chargingContextSpecific.get("16777416");
                        break;
                }

                if (serviceSession != null) record.put("EVENT_REF_NUM", serviceSession);
                break;

            case "GY_PRE":
            case "GY_POST":
                orgBNum = chargingContextSpecific.get("16777416");
                Object apn = chargingContextSpecific.get("16778235");
                if (apn != null) record.put("apn", apn);

                Object pdpAddress = chargingContextSpecific.get("16778228");
                if (pdpAddress != null) record.put("PDP_ADDRESS", pdpAddress);

                Object sgsnAddress = chargingContextSpecific.get("16778229");
                if (sgsnAddress != null) record.put("SGSN_ADDRESS", sgsnAddress);

                Object ggsnAddress = chargingContextSpecific.get("16778230");
                if (ggsnAddress != null) record.put("GGSN_ADDRESS", ggsnAddress);

                Object ratingGroup = chargingContextSpecific.get("16778254");
                if (ratingGroup != null) record.put("RATING_GROUP", ratingGroup);

                Object ctx16778233 = chargingContextSpecific.get("16778233");
                if (ctx16778233 != null) record.put("3GPP_GGSN_MCC_MNC", ctx16778233);

                Object ctx16778239 = chargingContextSpecific.get("16778239");
                if (ctx16778239 != null) record.put("3GPP_SGSN_MCC_MNC", ctx16778239);

//                record.put("ORIGINAL_DUR", 0);
                record.put("ZERO_DURATION_IND", origDur == 0 ? "1" : "0");
                if (totalOctetsUnit != null) billableVolume = (double) Long.parseLong(totalOctetsUnit.toString()) / 1024;

                record.put("BILLABLE_VOLUME", ((long) Math.ceil(billableVolume)));
                if (serviceSession != null) record.put("EVENT_REF_NUM", serviceSession);
                break;
            case "SIP_PRE":
            case "SIP_POST":
                orgBNum = chargingContextSpecific.get("16778219");
                Object userSessionID = r.get("userSessionID");
                Object iMSChargingIdentifier = r.get("iMSChargingIdentifier");
                if(iMSChargingIdentifier != null) record.put("EVENT_REF_NUM", iMSChargingIdentifier.toString());
                if(userSessionID != null) record.put("USER_SESSION_ID", userSessionID.toString());
                Object ctx150996140 = chargingContextSpecific.get("150996140");
                if(ctx150996140 != null){
                    ArrayList<String> callerSipIDs =  (ArrayList) ctx150996140;
                    for(String callerSipId : callerSipIDs){
                        if(callerSipId.startsWith("sip:")){
                            record.put("SERVED_MSISDN_SIP", callerSipId);
                        } else if(callerSipId.startsWith("tel:")) {
                            record.put("SERVED_MSISDN_TEL", callerSipId);
                        }
                    }
                }
                Object otherMSISDNSipId = chargingContextSpecific.get("16778413");
                if(otherMSISDNSipId != null) record.put("OTHER_MSISDN_SIP", otherMSISDNSipId.toString());
                record.put("ZERO_DURATION_IND", origDur == 0 ? "1" : "0");
            default:
                break;
        }

        Object lrn = chargingContextSpecific.get("16778220");
        if (lrn != null)
            record.put("LRN", lrn);

        String otherMSISDN = "";
        if (orgBNum != null) {
            record.put("ORIGINAL_B_NUM", orgBNum);
            if (isNumeric(orgBNum.toString().trim())){
                otherMSISDN = transformationLib.normalizeCCN(orgBNum.toString(), countryCode);
                if (otherMSISDN.startsWith("123")){
                    otherMSISDN = countryCode + otherMSISDN;
                }
            }else if (String.valueOf(orgBNum).trim().startsWith("81b") || String.valueOf(orgBNum).trim().startsWith("81123")){
                otherMSISDN = countryCode + String.valueOf(orgBNum).trim().substring(2);
            }
//            else {
//                otherMSISDN = countryCode + orgBNum.toString().trim();
//            }
            if (otherMSISDN != null && !otherMSISDN.trim().isEmpty()) {
                record.put("OTHER_MSISDN", otherMSISDN);
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(otherMSISDN);
                if (ddk != null) {
                    record.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                    record.put("EVENT_CATEGORY_KEY", ddk.getEventCategoryKey());
                    record.put("OTHER_MSISDN_ISO_CODE", ddk.getIsoCountryCode());
                    record.put("OTHER_MSISDN_DEST_TYPE", ddk.getDialDigitDesc());
                    String otherOper = ddk.getProviderDesc();
                    if (otherOper != null && otherOper.trim().length() > 0) {
                        record.put("OTHER_OPER", otherOper);
                    }

                    String nwIndKey;
                    if("DOMESTIC".equalsIgnoreCase(ddk.getDialDigitDesc())) {
                        if("125".equalsIgnoreCase(ddk.getNopIdKey())) nwIndKey = "1";
                        else nwIndKey = "2";
                    } else {
                        nwIndKey = "3";
                    }
                    record.put("NW_IND_KEY", nwIndKey);

                    if (Arrays.asList("SCF_PRE", "SCF_POST").contains(recType.toUpperCase())
                            && "3".equalsIgnoreCase(nwIndKey)
                            && charge > 0) {
                        String dialCode = ddk.getTargetCountryCode();
                        record.put("DIAL_CODE", dialCode);
                        /* min_pulse_ind, chargeable_unit_sec, avg_tariff using dial_code*/
                        MIntlZone zone = IntlZoneConfigManager.instance().get(dialCode);
                        if (zone != null) {
                            if ("true".equalsIgnoreCase(zone.getMinPulse())) record.put("MIN_PULSE", "1");
                            else record.put("MIN_PULSE", "0");
                            record.put("ZONE", zone.getZone());
                            double rfa = 0;
                            if (record.containsKey("CHARGE")) {
                                rfa = Double.parseDouble(String.valueOf(record.get("CHARGE")));
                            }
                            if ("true".equalsIgnoreCase(zone.getMinPulse())) {
                                record.put("CHARGEABLE_UNITS", billablePulse);
                                if (billablePulse > 0) record.put("AVG_TARIFF", rfa / billablePulse);
                            } else {
                                record.put("CHARGEABLE_UNITS", origDur);
                                if (origDur > 0) record.put("AVG_TARIFF", rfa / origDur);
                            }
                        } else {
                            logger.info("[CCN SCF PRE/POST Interconnect Traffic] Urgent action required. Dial Code Mapping not found for - {}", dialCode);
                        }
                    }
                }
            }
        }

        if (eventTypeKey.equals("4")) {
            billablePulse = 0;
        }

        record.put("BILLABLE_PULSE", billablePulse);
        record.put("ZERO_CHRG_IND", charge == 0 ? "1" : "0");

        Object orgANo = record.get("servedMSISDN");
        String SERVED_MSISDN = "";
        if (orgANo != null) {
            record.put(("ORIGINAL_A_NUM"), orgANo.toString());

            SERVED_MSISDN = transformationLib.normalizeCCN(orgANo.toString(), countryCode);
            record.put("SERVED_MSISDN", SERVED_MSISDN);

            ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(SERVED_MSISDN);
            if (ddk != null) {
                record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                record.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
            }
        }

        Object serviceCenterAddress = chargingContextSpecific.get("16777420");
        Object otherParthIP = chargingContextSpecific.get("16777421");

        if (serviceCenterAddress != null) {
            record.put("SERVICE_CENTRE_ADDRESS", serviceCenterAddress.toString());
        }
        if (otherParthIP != null) {
            record.put("OTHER_PARTY_IP", otherParthIP.toString());
        }
        record.put("POPULATION_DATE_TIME", sdfT.get().format(currentTime));

        Map<String, String> da = dedicatedAccountsHandler.handleDedicatedAccounts();
        record.putAll(da);

        Map<String, String> acc = accumulatorsHandler.handleAccumulators();
        record.putAll(acc);

//        String eventCatKey = getEventCatKey(eventTypeKey, serviceScenario, otherMSISDN);
//        if (eventCatKey != null) records.put("EVENT_CATEGORY_KEY", eventCatKey);
        String chargeUnitIDKey = transformationLib.getChrgUnitIdKey(eventTypeKey);
        record.put("CHRG_UNIT_ID_KEY", chargeUnitIDKey);

        String srvTypeKey = transformationLib.getCCNSrvTypeKey(
                serviceIdentifier.toString(), serviceScenario.toString(), roamingPosition.toString(), sysIdKey);
        if (srvTypeKey != null) {
            if(subType.equalsIgnoreCase("POST")){
                if(srvTypeKey.equals("2")) srvTypeKey = "1";
                else if(srvTypeKey.equals("6")) srvTypeKey = "5";
            }
            record.put("SRV_TYPE_KEY", String.valueOf(srvTypeKey));
        }

        String eventDirKey = transformationLib.getCCNEventDirectionKey(
                serviceIdentifier.toString(), serviceScenario.toString(), roamingPosition.toString(), sysIdKey);
        if(recType.equalsIgnoreCase("gy_pre") || recType.equalsIgnoreCase("gy_post")){
            if(eventDirKey == null && serviceIdentifier != null){
                eventDirKey = "1"; //GPRS Data, MMS will come from lookup
            }
        } else if(recType.equalsIgnoreCase("sip_pre") || recType.equalsIgnoreCase("sip_post")) {
            eventDirKey = "1";
        }
        record.put("EVENT_DIRECTION_KEY", eventDirKey);

        String nodeName = record.get("nodeName").toString();
        String neIdKey = transformationLib.getNEIdKey(nodeName);
        record.put("NE_ID_KEY", neIdKey);

        if (partialSequenceNumber == null) {
            record.put("REC_TYPE_ID_KEY", "4");
        } else {
            record.put("REC_TYPE_ID_KEY", "3");
        }

        int ussd;
        if ("6".equals(serviceScenario)) {
            ussd = 1;
        } else {
            ussd = 0;
        }
        record.put("SERVICE_SCENARIO_IND", String.valueOf(ussd)); // SERVICESCENARIO =6 (USSD CALL) then ‘1’ else ‘0’
        Double chargeBeforeTax;

        /*For postpaid no charge calculation is required*/
        if(subType.equalsIgnoreCase("pre")) {
            chargeBeforeTax = transformationLib.getChargeWithoutTax(charge);
        }else{
            chargeBeforeTax = charge;
        }
        if (chargeBeforeTax != null) {
            record.put("chargeBeforeTax", df.format(chargeBeforeTax));
            double tax = charge - chargeBeforeTax;
            record.put("tax", df.format(tax));
        }

        Object originatingLocationInfo = record.get("originatingLocationInfo");
        String cgiIDKey = getCGIIdKey(serviceIdentifierVal, originatingLocationInfo, chargingContextSpecific);

        Object lastCellID = getLastCell(serviceIdentifierVal, chargingContextSpecific);

        String nodeAddress;
//        if (serviceIdentifierVal > 65525 && serviceIdentifierVal < 65536)
//            nodeAddress = getNodeAddress(cgiIDKey, lastCellID, "17");
//        else
        nodeAddress = getNodeAddress(cgiIDKey, lastCellID, "17");
        if (nodeAddress != null) {
            record.put("NODE_ADDRESS", nodeAddress);
        }

        if (cgiIDKey != null) {
            record.put("CGI_ID_KEY", cgiIDKey);
        }

        if (lastCellID != null) {
            record.put("LAST_CELL_ID", lastCellID.toString());
        }

        Object ctx16778243 = chargingContextSpecific.get("16778243");
        if (ctx16778243 != null) {
            record.put("SERVED_IMEI", ctx16778243);
            record.put("TAC", ctx16778243.toString().substring(0, 8));
        }

        Object servedIMSI = record.get("servedIMSI");
        if (servedIMSI != null)
            record.put("SERVED_IMSI", servedIMSI.toString().trim());

        if (serviceClassID != null) {
            String subscription = transformationLib.getSubscriptionKey(serviceClassID.toString().trim());
            if (subscription != null) {
                record.put("SUBSCRIPTION_KEY", subscription);
            }
        }

        Object familyAndFriendsID = record.get("familyAndFriendsID");
        String faf = "0";
        if (familyAndFriendsID != null && !familyAndFriendsID.toString().trim().isEmpty())
            faf = "1";
        record.put("FAF_INDICATOR", faf);

        String oMSISDN = (otherMSISDN == null) ? "" : otherMSISDN;
        String sMSISDN = (SERVED_MSISDN == null) ? "" : SERVED_MSISDN;
        String eTypeKey = (eventTypeKey == null) ? "" : eventTypeKey;
        String eDirKey = (eventDirKey == null) ? "" : eventDirKey;
        String tTime = sdfT.get().format(triggerTime);
        String EDR_ID_KEY = eTypeKey + '|' + eDirKey + '|' + sMSISDN
                + '|' + oMSISDN + '|' + tTime + '|' + origDur;

        record.put("EDR_ID_KEY", EDR_ID_KEY);

        // xdr_date enrichment
        try {
//            String xdrDate = yyyymmdd.get().format(sdfT.get().parse(record.get("EVENT_START_TIME").toString()));
//            StringUtility.arrayContainsIgnoreCase(new String[]{"376535807", "377217763", "377214105", "377221277"}, String.valueOf(record.get("recordIdentificationNumber")))

            /* backlog changes commented */
//            String dayId = yyyymmdd.get().format(sdfT.get().parse(record.get("EVENT_START_TIME").toString()));
//            List<String> allowedDays = Arrays.asList("20220320", "20220321", "20220322", "20220323", "20220324", "20220325", "20220326");
//            if (!allowedDays.contains(dayId)){ // TODO Skip if cdr belongs to old days
//                return null;
//            }

            String xdrDate = sdfT.get().format(sdfT.get().parse(record.get("EVENT_START_TIME").toString()));
            String evStart = sdfT2.get().format(sdfT.get().parse(record.get("EVENT_START_TIME").toString()));
            String evEnd = sdfT2.get().format(sdfT.get().parse(record.get("EVENT_END_TIME").toString()));
            record.put("EVENT_START_TIME_KRT", record.get("EVENT_START_TIME").toString());
            record.put("EVENT_START_TIME", evStart);
            record.put("EVENT_END_TIME", evEnd);
            record.put("XDR_DATE", xdrDate);
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
//        EVENT_TYPE_KEY, EVENT_DIRECTION_KEY, SERVED_MSISDN, OTHER_ MSISDN , EVENT_START_TIME, ORIGINAL_DUR
        return record;
    }


    private String getCGIIdKey(long serviceIdentifierVal, Object originatingLocationInfo, LinkedHashMap<String, Object> chargingContextSpecific) {
        Object ctx16778217 = chargingContextSpecific.get("16778217");
        if (ctx16778217 != null) {
            if (!isNumeric(ctx16778217.toString()) && isHexNumber(ctx16778217.toString())) {
                ctx16778217 = String.valueOf(hex2Decimal(ctx16778217.toString()));
            }
        }

        Object ctx16778219 = chargingContextSpecific.get("16778219");
        if (ctx16778219 != null) {
            if (!isNumeric(ctx16778219.toString()) && isHexNumber(ctx16778219.toString())) {
                ctx16778219 = String.valueOf(hex2Decimal(ctx16778219.toString()));
            }
        }

        Object ctx16777423 = chargingContextSpecific.get("16777423");
        if (ctx16777423 != null) {
            if (!isNumeric(ctx16777423.toString()) && isHexNumber(ctx16777423.toString())) {
                ctx16777423 = String.valueOf(hex2Decimal(ctx16777423.toString()));
            }
        }
        if (originatingLocationInfo != null && originatingLocationInfo.toString().length() > 4) {
            originatingLocationInfo = originatingLocationInfo.toString().substring(4);
        }

        String cgiIDKey = null;
//        if (((serviceIdentifierVal > 65525 && serviceIdentifierVal < 65536))
//                || serviceIdentifierVal == 11 || serviceIdentifierVal == 12
//                || serviceIdentifierVal == 13) {
//            if (ctx16778217 != null) {
//                cgiIDKey = ctx16778217.toString();
//            }
//        }
        if (ctx16778217 != null) {
            cgiIDKey = ctx16778217.toString();
        }

        if (cgiIDKey == null && ctx16778219 != null) {
            cgiIDKey = ctx16778219.toString();
        }
//        else if (cgiIDKey != null && ctx16778219 != null)
//            cgiIDKey = cgiIDKey + '|' + ctx16778219.toString();

        if (cgiIDKey == null && ctx16777423 != null) {
            cgiIDKey = ctx16777423.toString();
        } else if (cgiIDKey != null && ctx16777423 != null) {
            cgiIDKey = cgiIDKey + '|' + ctx16777423.toString();
        }

        if (originatingLocationInfo != null) {
            cgiIDKey = originatingLocationInfo.toString();
        }

        return cgiIDKey;
    }

    private String getLastCell(long serviceIdentifierVal, LinkedHashMap<String, Object> chargingContextSpecific) {
        Object ctx16778249 = chargingContextSpecific.get("16778249");
        if (ctx16778249 != null && !isNumeric(ctx16778249.toString())) {
            if (isHexNumber(ctx16778249.toString())) {
                ctx16778249 = String.valueOf(hex2Decimal(ctx16778249.toString()));
            }
        }

        Object ctx16778250 = chargingContextSpecific.get("16778250");
        if (ctx16778250 != null && !isNumeric(ctx16778250.toString())) {
            if (isHexNumber(ctx16778250.toString())) {
                ctx16778250 = String.valueOf(hex2Decimal(ctx16778250.toString()));
            }
        }

        Object ctx16778251 = chargingContextSpecific.get("16778251");
        if (ctx16778251 != null && !isNumeric(ctx16778251.toString())) {
            if (isHexNumber(ctx16778251.toString())) {
                ctx16778251 = String.valueOf(hex2Decimal(ctx16778251.toString()));
            }
        }

        String lastCellID = null;
        if (serviceIdentifierVal > 65525 && serviceIdentifierVal < 65536) {
            if (ctx16778249 != null) {
                lastCellID = ctx16778249.toString();
            }

            if (ctx16778250 != null && lastCellID != null) {
                lastCellID = lastCellID + '|' + ctx16778250.toString();
            }

            if (ctx16778251 != null && lastCellID != null) {
                lastCellID = lastCellID + '|' + ctx16778251.toString();
            }

        } else {
            Object ctx16777423 = chargingContextSpecific.get("16777423");
            if (ctx16777423 != null) {
                if (!isNumeric(ctx16777423.toString())) {
                    if (isHexNumber(ctx16777423.toString())) {
                        ctx16777423 = String.valueOf(hex2Decimal(ctx16777423.toString()));
                    }
                }
                lastCellID = ctx16777423.toString();
            }
        }
        return lastCellID;
    }

    private String getNodeAddress(Object cgiIDKey, Object lastCellID, String s) {
        String nodeAddress = null;
        if (cgiIDKey != null) {
            nodeAddress = transformationLib.getDimLookupBestMatch(cgiIDKey.toString(), s);
        } else if (lastCellID != null) {
            nodeAddress = transformationLib.getDimLookupBestMatch(lastCellID.toString(), s);
        }
        return nodeAddress;
    }

    private String getEventCatKey(String eventTypeKey, String serviceScenario, String otherMSISDN) {
        String eventCatKey = null;
        if (eventTypeKey.equals("4")) {
            eventCatKey = "-98";
        } else {
            if (serviceScenario != null && otherMSISDN != null) {
                if (!serviceScenario.equals("2") && otherMSISDN.startsWith(countryCode)) {
                    eventCatKey = transformationLib.getDimLookup(otherMSISDN.substring(3), "11");
                }
            } else {
                eventCatKey = transformationLib.getDimEventCategoryBestMaich(otherMSISDN);
            }
        }
        return eventCatKey;
    }


    List<Object> getOLCCValueElements(ArrayList<LinkedHashMap<String, Object>> creditControlRecords, String name) {
        return creditControlRecords.stream()
                .filter(e -> e.containsKey(name))
                .map(e -> e.get(name)).collect(Collectors.toList());
    }

    List<Object> getValueElements(List<Map<String, Object>> creditControlRecords, String name) {
        return creditControlRecords.stream()
                .filter(e -> e.containsKey(name))
                .map(e -> e.get(name)).collect(Collectors.toList());
    }

    List<Map<String, Object>> getNodeElements(List<Map<String, Object>> creditControlRecords, String name) {
        List<Map<String, Object>> all = new ArrayList<>();
        List<Object> l = creditControlRecords.stream()
                .filter(e -> e.containsKey(name))
                .map(e -> e.get(name)).collect(Collectors.toList());

        for (Object lo : l) {
            if (lo != null && lo instanceof ArrayList) {
                ArrayList al = (ArrayList) lo;
                for (Object o : al) {
                    Map<String, Object> m = (Map<String, Object>) o;
                    all.add(m);
                }
            }
        }
        return all;
    }

    List<Map<String, Object>> getOLCCNodeElements(ArrayList<LinkedHashMap<String, Object>> creditControlRecords, String name) {
        List<Map<String, Object>> all = new ArrayList<>();
        List<Object> l = creditControlRecords.stream()
                .filter(e -> e.containsKey(name))
                .map(e -> e.get(name)).collect(Collectors.toList());

        for (Object lo : l) {
            if (lo instanceof ArrayList) {
                ArrayList al = (ArrayList) lo;
                for (Object o : al) {
                    Map<String, Object> m = null;
                    if( o instanceof ArrayList) {
                        List<Object> usedServiceUnits = (List<Object>) o;
                        m = (Map<String, Object>) usedServiceUnits.stream().findFirst().get();
                    } else if (o instanceof LinkedHashMap){
                        m = (Map<String, Object>) o;
                    }
                    all.add(m);
                }
            }
        }
        return all;
    }

    List<Object> getCCAccountData(List<List<Map<String, Object>>> legs, String elementName) {
        ArrayList<Object> all = new ArrayList<>();
        legs.stream().map((element) -> element.stream()
                .filter(e -> e.containsKey(elementName))
                .map(e -> e.get(elementName))
                .collect(Collectors.toList()))
                .filter(
                        (list) -> (!list.isEmpty())).forEachOrdered((list) -> {
            all.addAll(list);
        });
        return all;
    }

    private Map<String, Object> flattenCreditControlRecords(ArrayList<LinkedHashMap<String, Object>> creditControlRecords) {
        Map<String, Object> ccRecords = new LinkedHashMap<>();
//        List<Object> serviceIdentifiers = getOLCCValueElements(creditControlRecords, "serviceIdentifier");
        Object serviceIdentifier = max(creditControlRecords, "serviceIdentifier");
        if (serviceIdentifier != null && !serviceIdentifier.toString().isEmpty()) {
            ccRecords.put("serviceIdentifier", serviceIdentifier);
        }

        List<Map<String, Object>> usedServiceUnits = getOLCCNodeElements(creditControlRecords, "usedServiceUnits");
        Object tariffChangeUsage = minimum(usedServiceUnits, "tariffChangeUsage");
        if (tariffChangeUsage != null && !tariffChangeUsage.toString().isEmpty()) {
            ccRecords.put("tariffChangeUsage", tariffChangeUsage);
        }

        Object timeUnit = sum(usedServiceUnits, "timeUnit");
        if (timeUnit != null && !timeUnit.toString().isEmpty()) {
            ccRecords.put("timeUnit", timeUnit);
        }

        Object totalOctetsUnit = sum(usedServiceUnits, "totalOctetsUnit");
        if (totalOctetsUnit != null && !totalOctetsUnit.toString().isEmpty()) {
            ccRecords.put("totalOctetsUnit", totalOctetsUnit);
        }

        List<Object> eventTimes = getOLCCValueElements(creditControlRecords, "serviceIdentifier");
        Object eventTime = max(creditControlRecords, "eventTime");
        if (eventTime != null && !eventTime.toString().isEmpty()) {
            ccRecords.put("eventTime", eventTime);
        }

        List<Object> triggerTimes = getOLCCValueElements(creditControlRecords, "triggerTime");
        Object triggerTime = min(creditControlRecords, "triggerTime");
        if (triggerTime != null && !triggerTime.toString().isEmpty()) {
            ccRecords.put("triggerTime", triggerTime);
        }

        List<Object> serviceScenarios = getOLCCValueElements(creditControlRecords, "serviceScenario");
        Object serviceScenario = min(creditControlRecords, "serviceScenario");
        if (serviceScenario != null && !serviceScenario.toString().isEmpty()) {
            ccRecords.put("serviceScenario", serviceScenario);
        }

//        List<Object> roamingPositions = getOLCCValueElements(creditControlRecords, "roamingPosition");
        Object roamingPosition = min(creditControlRecords, "roamingPosition");
        if (roamingPosition != null && !roamingPosition.toString().isEmpty()) {
            ccRecords.put("roamingPosition", roamingPosition);
        }

        Object serviceSetupResult = min(creditControlRecords, "serviceSetupResult");
        if (serviceSetupResult != null && !serviceSetupResult.toString().isEmpty()) {
            ccRecords.put("serviceSetupResult", serviceSetupResult);
        }

        Object terminationCause = min(creditControlRecords, "terminationCause");
        if (terminationCause != null && !terminationCause.toString().isEmpty()) {
            ccRecords.put("terminationCause", terminationCause);
        }

        List<Map<String, Object>> selectionTreeParameters = getOLCCNodeElements(creditControlRecords, "selectionTreeParameter");

        List<Map<String, Object>> cCAccountDatas = getOLCCNodeElements(creditControlRecords, "cCAccountData");
        {
            List<Object> servedAccount = getValueElements(cCAccountDatas, "servedAccount");
            if (!servedAccount.isEmpty()) {
                ccRecords.put("servedAccount", servedAccount.get(0));
            }

            List<Object> serviceClassID = getValueElements(cCAccountDatas, "serviceClassID");
            if (!serviceClassID.isEmpty()) {
                ccRecords.put("serviceClassID", serviceClassID.get(0));
            }

            List<Object> accountGroupID = getValueElements(cCAccountDatas, "accountGroupID");
            if (!accountGroupID.isEmpty()) {
                ccRecords.put("accountGroupID", accountGroupID.get(0));
            }

            keepAccumulators(cCAccountDatas);
            keepDedicatedAccounts(cCAccountDatas);

            List<Map<String, Object>> accountValueBeforeL = getNodeElements(cCAccountDatas, "accountValueBefore");
            Object accountValueBefore = maximum(accountValueBeforeL, "amount");
            if (accountValueBefore != null && !accountValueBefore.toString().isEmpty()) {
                ccRecords.put("accountValueBefore", accountValueBefore);
            }

            List<Map<String, Object>> accountValueAfterL = getNodeElements(cCAccountDatas, "accountValueAfter");
            Object accountValueAfter = minimum(accountValueAfterL, "amount");
            if (accountValueAfter != null && !accountValueAfter.toString().isEmpty()) {
                ccRecords.put("accountValueAfter", accountValueAfter);
            }

            List<Object> familyAndFriendsNoL = getValueElements(cCAccountDatas, "familyAndFriendsNo");
            if (!familyAndFriendsNoL.isEmpty()) {
                ccRecords.put("familyAndFriendsNo", familyAndFriendsNoL.get(0));
            }

            List<Object> familyAndFriendsIDL = getValueElements(cCAccountDatas, "familyAndFriendsID");
            if (!familyAndFriendsIDL.isEmpty()) {
                ccRecords.put("familyAndFriendsID", familyAndFriendsIDL.get(0));
            }

            List<Object> serviceOfferings = getValueElements(cCAccountDatas, "serviceOfferings");
            if (!serviceOfferings.isEmpty()) {
                ccRecords.put("serviceOfferings", serviceOfferings.get(0));
            }

            List<Object> accumulatedUnits = getValueElements(cCAccountDatas, "accumulatedUnits");
            if (!accumulatedUnits.isEmpty()) {
                ccRecords.put("accumulatedUnits", accumulatedUnits.get(0));
            }

            List<Object> accountUnitsDeducted = getValueElements(cCAccountDatas, "accountUnitsDeducted");
            if (!accountUnitsDeducted.isEmpty()) {
                ccRecords.put("accountUnitsDeducted", accountUnitsDeducted.get(0));
            }

            List<Map<String, Object>> accumulatedCost = getNodeElements(cCAccountDatas, "accumulatedCost");
            if (!accumulatedCost.isEmpty()) {
                ccRecords.put("accumulatedCost", sum(accumulatedCost, "amount"));
            }

            List<Map<String, Object>> accountValueDeducted = getNodeElements(cCAccountDatas, "accountValueDeducted");
            if (!accountValueDeducted.isEmpty()) {
                ccRecords.put("accountValueDeducted", sum(accountValueDeducted, "amount"));
            }
        }
        List<List<Object>> chargingContextSpecifics = creditControlRecords.stream()
                .filter(e -> e.containsKey("chargingContextSpecific"))
                .map(e -> (ArrayList<Object>) e.get("chargingContextSpecific")).collect(Collectors.toList());
        Map chargingContextSpecific = flattenChargingContextSpecific(chargingContextSpecifics);
        ccRecords.put("chargingContextSpecific", chargingContextSpecific);

        List<List<Object>> treeDefinedFieldss = creditControlRecords.stream()
                .filter(e -> e.containsKey("treeDefinedFields"))
                .map(e -> (ArrayList<Object>) e.get("treeDefinedFields")).collect(Collectors.toList());
        Map treeDefinedFields = flattenTreeDefinedFields(treeDefinedFieldss);
        ccRecords.put("treeDefinedFields", treeDefinedFields);

        return ccRecords;
    }

    private void keepAccumulators(List<Map<String, Object>> cCAccountDatas) {
        try {
            List<Map<String, Object>> accumulators = cCAccountDatas.stream()
                    .filter(e -> e.containsKey("accumulators"))
                    .collect(Collectors.toList());
            accumulatorsHandler.setAccuAccount(accumulators);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void keepDedicatedAccounts(List<Map<String, Object>> cCAccountDatas) {
        List<List<List<Map<String, Object>>>> dedicatedAccounts = cCAccountDatas.stream()
                .filter(e -> e.containsKey("dedicatedAccounts"))
                .map(e -> (List<List<Map<String, Object>>>) e.get("dedicatedAccounts"))
                .collect(Collectors.toList());
        dedicatedAccounts.forEach(e -> {
            dedicatedAccountsHandler.dedicatedAccounts.addAll(e);
        });
    }

    public List<Map<String, Object>> flattenListOfLists(List<List<Map<String, Object>>> listOfLists) {
        List<Map<String, Object>> all = new ArrayList<>();
        listOfLists.forEach(list -> all.addAll(list));
        return all;
    }

    private Map<String, Object> flattenTreeDefinedFields(List<List<Object>> treeDefinedFieldss) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        if (treeDefinedFieldss != null) {
            treeDefinedFieldss.forEach((leg) -> {
                for (Object el : leg) {
                    Map<String, Object> element = ((ArrayList<Map<String, Object>>) el).stream().findFirst().get();
                    String PIDTreeDefined = element.get("PIDTreeDefined").toString();
                    ArrayList<LinkedHashMap<String, Object>> paramVal
                            = (ArrayList<LinkedHashMap<String, Object>>) element.get("paramVal");
                    for (Map.Entry<String, Object> e : paramVal.get(0).entrySet()) {
                        String context = e.getKey();
                        Object value = e.getValue();
                        m.put(PIDTreeDefined, value);
                    }
                }
            });
        }
        return m;
    }

    private LinkedHashMap<String, Object> flattenChargingContextSpecific(List<List<Object>> ctxParamSpec) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        if (ctxParamSpec != null) {
            for (List<Object> leg : ctxParamSpec) {
                for (Object el : leg) {
                    Map<String, Object> element = ((ArrayList<Map<String, Object>>) el).stream().findFirst().get();
                    String ctxParameterID = element.get("ctxParameterID").toString();
                    ArrayList<LinkedHashMap<String, Object>> val
                            = (ArrayList<LinkedHashMap<String, Object>>) element.get("contextParameterValueType");
                    for (Map.Entry<String, Object> e : val.get(0).entrySet()) {
                        String k = e.getKey();
                        Object value =  e.getValue();
                        m.put(ctxParameterID, value);
                    }
                }
            }
        }
        return m;
    }

    public static String readCharValue(byte[] ints, int index, int len) {
        int i = index;
        int length = index + len;

        char[] chrs = new char[len];
        while (i < length) {
            chrs[(i - index)] = ((char) ints[i]);
            if (chrs[(i - index)] == 0) {
                chrs[(i - index)] = ' ';
            }
            i++;
        }
        return new String(chrs).trim();
    }

    private static Object msisdnContextVal(byte[] result) {
        String var1 = getHexValue(result, 0, 1);
        String var2 = convertHexToTBCDValue(result, 1, result.length, 15);
        return var1 + var2;
    }

    public static String convertHexToTBCDValue(byte[] result, int _index, int _intLength, int _filler) {
        StringBuffer tbcdString = new StringBuffer();
        int temp = 0;

        int endIndex = _index + _intLength < result.length ? _index + _intLength : result.length;
        for (int i = _index; i < endIndex; i++) {
            int x = result[i] & 0xF;
            temp = result[i] & 0xF;

            tbcdString.append(Integer.toHexString(temp));

            x = (result[i] & 0xF0) >>> 4;
            temp = (result[i] & 0xF0) >>> 4;

            tbcdString.append(Integer.toHexString(temp));
        }
        if ((tbcdString.toString().endsWith("f")) || (tbcdString.toString().endsWith("F"))) {
            return tbcdString.toString().substring(0, tbcdString.length() - 1);
        }

        return tbcdString.toString();
    }

    private static Object sContextVal(byte[] op) {
        String val = getHexValue(op, 0, op.length);

        String val1 = "";
        boolean isJunk = false;
        for (byte anOp : op) {
            if (!isJunk && anOp < 32)
                isJunk = true;
            val1 = val1 + removeFF(Integer.toHexString(anOp));
        }

        return !isJunk ? new String(op) : val1;
    }

    public static String getHexValue(int[] ints, int index, int intLength) {
        StringBuilder value = new StringBuilder();
        int i = index;
        int length = index + intLength < ints.length ? index + intLength : ints.length;
        while (i < length) {
            if (ints[i] <= 15)
                value.append("0");
            value.append(Integer.toHexString(ints[i]));
            i++;
        }
        return value.toString();
    }

    public static String getHexValue(byte[] bytes, int idx, int len) {
        StringBuilder value = new StringBuilder();
        int endIndex = idx + len < bytes.length ? idx + len : bytes.length;
        int i = idx;
        while (i < endIndex) {
            int left = (bytes[i] & 0xF0) >>> 4;
            int right = bytes[i] & 0xF;
            int2Hex(value, left);
            int2Hex(value, right);
            i++;
        }
        return value.toString();
    }

    public static void int2Hex(StringBuilder bcd, int num) {
        if (num <= 9) bcd.append(num);
        else if (num == 10) bcd.append("A");
        else if (num == 11) bcd.append("B");
        else if (num == 12) bcd.append("C");
        else if (num == 13) bcd.append("D");
        else if (num == 14) bcd.append("E");
        else if (num == 15) bcd.append("F");
    }

    public static String removeFF(String s) {
        if (s.length() > 2)
            return s.substring(s.length() - 2);
        return s;
    }

    private static String aStringContextVal(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append((bytes[0] & 0x80) >>> 7);
        sb.append((bytes[0] & 0x70) >>> 4);
        int npiInt = bytes[0] & 0xF;
        if (npiInt > 9) sb.append(npiInt);
        else {
            sb.append('0');
            sb.append(npiInt);
        }
        for (int i = 1; i < bytes.length; i++) {
            sb.append(bytes[i] & 0xF);
            sb.append((bytes[i] & 0xF0) >>> 4);
        }
        return sb.toString();
    }

    Object u64Context(byte[] bytes) {
        return i2Context(bytes);
    }

    Object u32Context(byte[] bytes) {
        return i2Context(bytes);
    }

    Object i4Context(byte[] bytes) {
        return i2Context(bytes);
    }

    Object i2Context(byte[] bytes) {
        Object val1 = hex2Long(bytes, 0, bytes.length);
        return val1;
    }

    private Object b1ContextVal(byte[] bytes) {
        Object result = new BigInteger(1, bytes);
        Object b = result.toString().trim().equals("255");
        return b;
    }

    private static String getHex(byte[] hexValues) {
        StringBuilder sb = new StringBuilder();
        for (byte hexValue : hexValues)
            sb.append(byte2Chars(hexValue));
        return sb.toString();
    }

    private static Object byte2Chars(byte bt) {
        StringBuilder hexValue = new StringBuilder();
        int c = (bt & 0xF0) >>> 4;
        if (c >= 10) hexValue.append((char) (c - 10 + 65));
        else hexValue.append(c);
        c = bt & 0xF;
        if (c >= 10) hexValue.append((char) (c - 10 + 65));
        else hexValue.append(c);

        return hexValue.toString();
    }

    private static Object hex2Long(byte[] ints, int index, int len) {
        long num = 0L;
        int length = index + len < ints.length ? index + len : ints.length;
        for (int i = index; i < length; i++) {
            num = num * 16L + ((ints[i] & 0xF0) >>> 4);
            num = num * 16L + (ints[i] & 0xF);
        }
        return num;
    }

    private String decodeOtherMSISDN(byte[] buffer) {
        String value = null;
        try {
            if (buffer != null && buffer.length > 0) {
                value = hex2BCD(buffer, 0, 1, 15);
                value += aStringContextVal(buffer, 1, buffer.length, 15).trim();
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
        return value;
    }

    private String hex2BCD(byte[] bytes, int index, int len, int _filler) {
        StringBuilder bcdString = new StringBuilder(0);
        int endIndex = ((index + len) < bytes.length) ? (index + len) : bytes.length;
        int i = index;
        while (i < endIndex) {
            int left = (bytes[i] & 0xF0) >>> 4;
            int right = bytes[i] & 0xF;
            if (right == _filler) {
                break;
            }
            int2Hex(bcdString, left);
            if (left == _filler) {
                break;
            }
            int2Hex(bcdString, right);
            i++;
        }
        return bcdString.toString().trim();
    }


    private String aStringContextVal(byte[] bytes, int index, int len, int _filler) {
        StringBuilder tbcdString = new StringBuilder(0);
        int endIndex = ((index + len) < bytes.length) ? (index + len) : bytes.length;
        int i = index;
        while (i < endIndex) {
            int left = (bytes[i] & 0xF0) >>> 4;
            int right = bytes[i] & 0xF;
            if (right == _filler) {
                break;
            }
            int2Hex(tbcdString, right);
            if (left == _filler) {
                break;
            }
            int2Hex(tbcdString, left);
            i++;
        }
        return tbcdString.toString().trim();
    }


    private double getAmount(LinkedHashMap<String, Object> avd) {
        double amount;
        long a = ((BigInteger) avd.get("amount")).longValue();
        long precision = ((BigInteger) avd.get("decimals")).longValue();
        amount = a / Math.pow(10, precision);
        return amount;
    }

    private Map<String, String> traverseServedSubscription(Object value) {
        Map<String, String> data = new LinkedHashMap<>();
        if (value instanceof ArrayList) {
            for (ArrayList<Object> valList: (ArrayList<ArrayList<Object>>) value) {
                LinkedHashMap<String, Object> r = (LinkedHashMap<String, Object>) valList.stream().findFirst().get();
                String subId = String.valueOf(r.get("subscriptionIDType")).trim();

                String val = String.valueOf(r.get("subscriptionIDValue")).trim();
                if (subId.equalsIgnoreCase("0")) {
                    data.put(("servedMSISDN"), val);
                } else if (subId.equalsIgnoreCase("1")) {
                    data.put(("servedIMSI"), val);
                }
            }
        }
        return data;
    }

    private Map<String, String> traverseServingElement(ArrayList<LinkedHashMap<String, Object>> value) {
        Map<String, String> data = new LinkedHashMap<>();
        LinkedHashMap<String, Object> object = value.get(0);
        for (Map.Entry<String, Object> entry : object.entrySet()) {
            if (entry.getKey().equalsIgnoreCase("originInfo")) {
                for (LinkedHashMap<String, Object> obj : (ArrayList<LinkedHashMap<String, Object>>) entry.getValue()) {
                    for (Map.Entry<String, Object> e : obj.entrySet()) {
                        data.put((e.getKey()), String.valueOf(e.getValue()));
                    }
                }
            } else if (entry.getKey().equalsIgnoreCase("mSCAddress")) {
                data.put((entry.getKey()), String.valueOf(entry.getValue()));
            } else if (entry.getKey().equalsIgnoreCase("ggsnAddress")) {
                data.put((entry.getKey()), String.valueOf(entry.getValue()));
            } else if (entry.getKey().equalsIgnoreCase("sgsnAddress")) {
                data.put((entry.getKey()), String.valueOf(entry.getValue()));
            } else {
                logger.info(entry.getKey() + " not handled");
            }
        }

        return data;
    }

    private Map<String, String> traverseCorrelationId(Object value) {
        Map<String, String> data = new LinkedHashMap<>();
        ArrayList<LinkedHashMap<String, Object>> r = (ArrayList<LinkedHashMap<String, Object>>) value;
        for (LinkedHashMap<String, Object> inn : r) {
            for (Map.Entry<String, Object> entry : inn.entrySet()) {
                String k = entry.getKey();
                Object v = entry.getValue();
                if (v instanceof ArrayList) {
                    LinkedHashMap<String, Object> v1 = ((ArrayList<LinkedHashMap<String, Object>>) v).get(0);
                    data.put((k), v1.values().toArray()[0].toString());
                } else {
                    data.put((k), v.toString());
                }
//                if (key.equalsIgnoreCase("dccCorrId") || key.equalsIgnoreCase("callReference")) {
//                    data.put((key), String.valueOf(entry.getValue()));
//                }
            }
        }
        return data;
    }

    private static boolean isHexNumber(String cadena) {
        try {
            Long.parseLong(cadena, 16);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private static int hex2Decimal(String s) {
        String digits = "0123456789ABCDEF";
        s = s.toUpperCase();
        int val = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int d = digits.indexOf(c);
            val = 16 * val + d;
        }
        return val;
    }

    public static boolean isNumeric(String str) {
//        for (char c : str.toCharArray()) {
//            if (!Character.isDigit(c)) {
//                return false;
//            }
//        }
        return NumberUtils.isNumber(str);
    }

    private String dsv(List<Map<String, Object>> listOfMap, String key) {
        String c = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .reduce("", (x, y) -> x + y + "|");
        if (c != null && c.length() > 1) {
            c = c.substring(0, c.length() - 1);
        }
        return c;
    }

    private Long sum(List<Map<String, Object>> listOfMap, String key) {
        return listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .filter(e -> isNumeric(e.get(key).toString().trim()))
                .map(e -> Long.parseLong(e.get(key).toString().trim()))
                .reduce(0l, (a, b) -> a + b);
    }

    private Long sum(List<Object> list) {
        return list.stream()
                .filter(e -> e != null && isNumeric(e.toString().trim()))
                .map(e -> Long.parseLong(e.toString().trim()))
                .reduce(0l, (a, b) -> a + b);
    }

    private Object min(List<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return null;
    }

    private Object minimum(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(0);
        }
        return "";
    }

    private Object maximum(List<Map<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }

    private Object max(ArrayList<LinkedHashMap<String, Object>> listOfMap, String key) {
        List<Object> l = listOfMap.stream()
                .map(e -> e.get(key).toString().trim())
                .sorted().collect(Collectors.toList());
        if (l.size() > 0) {
            return l.get(l.size() - 1);
        }
        return "";
    }


    private static <T> Predicate<T> distinct(Function<? super T, Object> key) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(key.apply(t), Boolean.TRUE) == null;
    }


}

package com.gamma.skybase.build.server.etl.tx.tap;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimSuscriberCRMInf;
import com.gamma.skybase.build.server.etl.utils.OpcoBusinessTransformation;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import com.gamma.telco.utility.MTAPRecordHeader;
import com.gamma.telco.utility.reference.ReferenceDimTadigLookup;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.gamma.telco.utility.TelcoBusinessTransformation.cache;

/**
 * Created by abhi on 09/28/2017
 */
@SuppressWarnings("unchecked")
public class TAPRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(TAPRecordEnrichment.class);

    private final DecimalFormat decimalFormat = new DecimalFormat("#.000000");
    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private final AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final String homePlmnCode = appConfig.getProperty("app.datasource.plmn");
    private final String localDateOffset = appConfig.getProperty("app.datasource.timeoffset");
    private Map<String, String> partnerDateOffsets;
    private String cdrType;
    private String sender;
    private String recipient;
    private ReferenceDimTadigLookup partnerPlmnDetails;

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();

        MTAPRecordHeader recordHeader = (MTAPRecordHeader) request.getOptionalParams();
        txRecord.putAll(getTapBasicInfo(recordHeader));
        partnerDateOffsets = recordHeader.getUtcOffsetInfo();

        Object eventType = record.get("eventType");
        txRecord.put("eventType", eventType);
        txRecord.put("FILE_NAME", record.get("fileName"));

        switch (eventType.toString().trim().toLowerCase()) {
            case "notification":
            case "batchcontrolinfo":
                txRecord.putAll(handleNotificationEvent(record));
                break;
            case "mobileoriginatedcall":
            case "mobileterminatedcall":
            case "gprscall":
                txRecord.putAll(handleBillableContents(eventType.toString().toLowerCase(), record, recordHeader));
                txRecord.put("OTHER_CURRENCY", recordHeader.getSenderCurrency());
                txRecord.put("EXCHANGE_RATE", recordHeader.getExchangeRate());
//                txRecord.put("TAP_DECIMAL_PLACES", recordHeader.getNumberOfDecimalPlaces());
                txRecord.put("TAP_DECIMAL_PLACES", recordHeader.getTapDecimalPlaces());
                break;
            case "messagedescriptioninfo":
            case "supplserviceevent":
            case "servicecentreusage":
            case "contenttransaction":
            case "locationservice":
            case "messagingevent":
            case "mobilesession":
                response.setResponseCode(false);
            default:
                break;
        }
        response.setResponseCode(true);
        response.setResponse(txRecord);
        return response;
    }

    private Map<String, Object> handleBillableContents(String eventType, LinkedHashMap<String, Object> record, MTAPRecordHeader recordHeader) {

        Map<String, Object> txRecord = new LinkedHashMap<>();

        String msisdn = record.get("Msisdn") == null ? "" : record.get("Msisdn").toString();
        String servedIMSI = record.get("Imsi") == null ? "" : record.get("Imsi").toString();
        String servedIMEI = record.get("Imei") == null ? "" : record.get("Imei").toString();

        if (StringUtils.isNotEmpty(msisdn) && msisdn.startsWith("00")) {
            msisdn = msisdn.substring(2);
        }
        txRecord.put("SERVED_MSISDN", msisdn);
        if (servedIMSI != null) txRecord.put("SERVED_IMSI", servedIMSI);
        if (servedIMEI != null) txRecord.put("SERVED_IMEI", servedIMEI);

        switch (eventType.toLowerCase()) {
            case "mobileoriginatedcall":
            case "mobileterminatedcall":
                handleVoiceSMSCallEvent(record, txRecord, recordHeader);
                break;
            case "gprscall":
                handleGprsCallEvent(record, txRecord, recordHeader);
                break;
            default:
                break;
        }
        return txRecord;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return null;
    }

    private Map<String, Object> handleNotificationEvent(Object object) {
        Map<String, Object> record = (Map<String, Object>) object;
        Map<String, Object> notification = new LinkedHashMap<>();
        notification.put("FILE_SEQUENCE_NUMBER", record.get("FileSequenceNumber"));
        notification.put("SPECIFICATION_VERSION_NUMBER", record.get("SpecificationVersionNumber"));
        notification.put("RELEASE_VERSION_NUMBER", record.get("ReleaseVersionNumber"));

        if (record.get("FileCreationTimeStamp") != null)
            notification.put("FILE_CREATION", record.get("FileCreationTimeStamp"));
        if (record.get("FileCreation") != null) notification.put("FILE_CREATION", record.get("FileCreation"));

        if (record.get("TransferCutOff") != null) notification.put("TRANSFER_CUT_OFF", record.get("TransferCutOff"));
        if (record.get("TransferCutOffTimeStamp") != null)
            notification.put("TRANSFER_CUT_OFF", record.get("TransferCutOffTimeStamp"));

        if (record.get("FileAvailableTimeStamp") != null)
            notification.put("FILE_AVAILABLE", record.get("FileAvailableTimeStamp"));
        if (record.get("FileAvailable") != null) notification.put("FILE_AVAILABLE", record.get("FileAvailable"));

//        notification.put("XDR_DATE", sdfT.get().format(localStartDateTime));
        notification.put("XDR_DATE", sdfT.get().format(new Date()));  //TODO temp fix
        notification.put("POPULATION_DATE", sdfT.get().format(new Date()));

        return notification;
    }

    private void handleVoiceSMSCallEvent(Object object, Map<String, Object> txRecord, MTAPRecordHeader recordHeader) {
        String msisdn = txRecord.get("SERVED_MSISDN").toString();
        String imsi = txRecord.get("SERVED_IMSI").toString();
        LinkedHashMap<String, Object> record = (LinkedHashMap<String, Object>) object;
        String otherMSISDN = "";
        String eventType = record.get("eventType").toString();
        if (eventType.equalsIgnoreCase("MobileOriginatedCall")) {
            if (record.get("CalledNumber") != null) {
                otherMSISDN = record.get("CalledNumber").toString();
            }
        } else {
            if (record.get("CallingNumber") != null) {
                otherMSISDN = record.get("CallingNumber").toString();
            }
        }
        String eventTypeKey = "";
        String eventDirectionKey = "";
        double charge = 0D;
        long chargeableUnits = 0L;
        long originalDuration = 0L;
        long billablePulse = 0L;

        //OLD
//        Object basicServiceUsed = record.get("BasicServiceUsed");
//        txRecord.putAll(handleBasicServiceUsed(basicServiceUsed));

        //NEW
        Object basicServiceUsedList = record.get("BasicServiceUsedList");
        txRecord.putAll(handleBasicServiceUsed(basicServiceUsedList));

        String callReference = "";
        if (record.get("CallReference") != null) callReference = record.get("CallReference").toString();
        txRecord.put("EVENT_REF_NUM", callReference);


        String teleServiceCode = txRecord.get("TeleServiceCode").toString();
        txRecord.put("BASIC_SERVICE_KEY", teleServiceCode);
        if (txRecord.get("Charge") != null) {
            charge = Double.parseDouble(txRecord.get("Charge").toString());
            txRecord.put("CHARGE", charge);
//            int tapDecimalPlaces = Integer.parseInt(recordHeader.getNumberOfDecimalPlaces());
        }

        switch (teleServiceCode) {
            //voice mt
            case "11":
            case "12":
                eventTypeKey = "1";
                originalDuration = (Long) txRecord.get("ChargedUnits");
                chargeableUnits = (Long) txRecord.get("ChargeableUnits");
                eventDirectionKey = eventType.equalsIgnoreCase("MobileOriginatedCall") ? "1" : "2";
                break;
            //sms mt
            case "21":
            case "22":
                eventTypeKey = "2";
                billablePulse = 1L;
                eventDirectionKey = eventType.equalsIgnoreCase("MobileOriginatedCall") ? "1" : "2";
                break;
            default:
                break;
        }

        txRecord.put("CHARGEABLE_UNITS", chargeableUnits);
        txRecord.put("ORIGINAL_DUR", originalDuration);
//        if(originalDuration > 0) billablePulse = (long) Math.ceil((double) originalDuration / 60);
        if(chargeableUnits > 0) billablePulse = (long) Math.ceil((double) chargeableUnits / 60);
        txRecord.put("BILLABLE_PULSE", billablePulse);


        txRecord.put("EVENT_TYPE_KEY", eventTypeKey);
        txRecord.put("EVENT_DIRECTION_KEY", eventDirectionKey);

        txRecord.put("SRV_TYPE_KEY", getSrvTypeKey(txRecord.get("SERVED_MSISDN").toString()));
//        txRecord.put("SRV_TYPE_KEY", getSrvTypeKey(msisdn, imsi));

        if (otherMSISDN != null && !otherMSISDN.trim().isEmpty()) {
            txRecord.put("OTHER_MSISDN", otherMSISDN);
            ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(otherMSISDN);
            if (ddk != null) {
                txRecord.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                txRecord.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                txRecord.put("EVENT_CATEGORY_KEY", ddk.getEventCategoryKey());
                txRecord.put("OTHER_MSISDN_ISO_CODE", ddk.getIsoCountryCode());
                txRecord.put("OTHER_OPER", ddk.getProviderDesc());
            }
        }

        String chargeUnitIDKey = transformationLib.getChrgUnitIdKey(eventTypeKey);
        txRecord.put("CHRG_UNIT_ID_KEY", chargeUnitIDKey);

        String zeroChargeInd = charge == 0 ? "1" : "0";
        txRecord.put("ZERO_CHRG_IND", zeroChargeInd);

        if (record.get("CallEventTimeStamp") != null) {
            handleTimestampTags(record, txRecord, originalDuration);
        }

        if (txRecord.get("Charge") != null) {
            try {
                String xdrDate = (String) txRecord.get("XDR_DATE");
                Date cdrDate = sdfT.get().parse(xdrDate);
                int tapDecimalPlaces = recordHeader.getTapDecimalPlaces();
                Double xdrCharge = charge / Math.pow(10, tapDecimalPlaces);
                String sdrValue = transformationLib.getSdrValue(cdrDate);
                if (sdrValue == null) sdrValue = "1";
                txRecord.put("SDR_CHARGE", decimalFormat.format(xdrCharge));
                txRecord.put("XDR_CHARGE", decimalFormat.format(xdrCharge * Float.parseFloat(sdrValue)));
            } catch (Exception e) {
            }
        }

    }


    private void handleGprsCallEvent(Object object, Map<String, Object> txRecord, MTAPRecordHeader recordHeader) {
        LinkedHashMap<String, Object> record = (LinkedHashMap<String, Object>) object;
        String eventTypeKey = "4";
        Long chargeableUnits = 0L;
        Long chargedVolume = 0L;
        Double charge = 0D;

        String msisdn = txRecord.get("SERVED_MSISDN").toString();
        String imsi = txRecord.get("SERVED_IMSI").toString();
        Long originalDuration = Long.parseLong(record.get("TotalCallEventDuration").toString());
        Long uploadVolume = Long.parseLong(record.get("DataVolumeIncoming").toString());
        Long downloadVolume = Long.parseLong(record.get("DataVolumeOutgoing").toString());
        Long totalVolume = downloadVolume + uploadVolume;
        String chargingId = record.get("ChargingId") == null ? "" : record.get("ChargingId").toString();
        String cellId = record.get("CellId") == null ? "" : record.get("CellId").toString();
        String lac = record.get("LocationArea") == null ? "" : record.get("LocationArea").toString();

        txRecord.put("PDP_ADDRESS", record.get("PdpAddress"));
        txRecord.put("APN_NI", record.get("AccessPointNameNI"));
        txRecord.put("APN_OI", record.get("AccessPointNameOI"));
        txRecord.put("EVENT_REF_NUM", chargingId);
        txRecord.put("CGI_CELL_ID", cellId);
        txRecord.put("CGI_LAC", lac);

        txRecord.put("SRV_TYPE_KEY", getSrvTypeKey(record.get("SERVED_MSISDN").toString()));

        Object accessPointNameNI = record.get("AccessPointNameNI");
        if (accessPointNameNI != null) {
            String apnNI = accessPointNameNI.toString().trim();
//            if (apnNI.equalsIgnoreCase("mms")) eventTypeKey = "20";   //OLD logic
            if (apnNI.equalsIgnoreCase("mms")) eventTypeKey = "3"; // sync with GPRS, CCN & Rated-TAP stream
        }

//        Object chargeInfoList = record.get("ChargeInformationList");
//        if (chargeInfoList != null) {
//            record.putAll(handleChargeInformation(chargeInfoList));
//        }

        Object infoList = record.get("ChargeInformationList");
        if (infoList != null) {
            record.putAll(handleChargeInformation(infoList));
        }

        txRecord.put("EVENT_TYPE_KEY", eventTypeKey);
        txRecord.put("EVENT_DIRECTION_KEY", "1");

        txRecord.put("ORIGINAL_DUR", originalDuration);
        txRecord.put("GPRS_UPLINK_VOLUME", uploadVolume);
        txRecord.put("GPRS_DOWNLINK_VOLUME", downloadVolume);
        txRecord.put("GPRS_TOTAL_VOLUME", totalVolume);

        if (record.get("ChargeableUnits") != null)
            chargeableUnits = Long.parseLong(String.valueOf(record.get("ChargeableUnits")));
        if (record.get("ChargedUnits") != null)
            chargedVolume = Long.parseLong(String.valueOf(record.get("ChargedUnits")));
        if (record.get("Charge") != null) {
            charge = Double.parseDouble(String.valueOf(record.get("Charge")));
            txRecord.put("CHARGE", charge);
        }

        txRecord.put("CHARGEABLE_UNITS", chargeableUnits);
        txRecord.put("CHARGED_VOLUME", chargedVolume);
//        long billablePulse = (long) Math.ceil((double) chargedVolume / 1024);
        long billablePulse = (long) Math.ceil((double) chargeableUnits / 1024);
        txRecord.put("BILLABLE_PULSE", billablePulse);

        String chargeUnitIDKey = transformationLib.getChrgUnitIdKey(eventTypeKey);
        txRecord.put("CHRG_UNIT_ID_KEY", chargeUnitIDKey);

        String zeroChargeInd = charge == 0 ? "1" : "0";
        txRecord.put("ZERO_CHRG_IND", zeroChargeInd);

        if (record.get("CallEventTimeStamp") != null) {
            handleTimestampTags(record, txRecord, originalDuration);
        }

        if (record.get("Charge") != null) {
            try {
                String xdrDate = (String) txRecord.get("XDR_DATE");
                Date cdrDate = sdfT.get().parse(xdrDate);
                int tapDecimalPlaces = recordHeader.getTapDecimalPlaces();
                Double xdrCharge = charge / Math.pow(10, tapDecimalPlaces);
                String sdrValue = transformationLib.getSdrValue(cdrDate);
                if (sdrValue == null) sdrValue = "1";
                txRecord.put("SDR_CHARGE", decimalFormat.format(xdrCharge));
                txRecord.put("XDR_CHARGE", decimalFormat.format(xdrCharge * Float.parseFloat(sdrValue)));
            } catch (ParseException e) {

            }
        }
    }

    private void handleTimestampTags(LinkedHashMap<String, Object> record, Map<String, Object> txRecord, Long originalDuration) {
        Object cdrRecordTimeTxt = record.get("CallEventTimeStamp");
        String utcOffsetCode = record.get("CallEventUtcTimeOffsetCode") == null ? "" : record.get("CallEventUtcTimeOffsetCode").toString();
        if (cdrRecordTimeTxt != null) {
            try {
                Date cdrRecordDate = sdfS.get().parse(cdrRecordTimeTxt.toString().trim());
                Date localStartDateTime = computeLocalDateTime(cdrRecordDate, utcOffsetCode);
                Date localEndDateTime = new Date(localStartDateTime.getTime() + originalDuration * 1000);

                txRecord.put("CDR_RECORD_TIME", sdfT.get().format(cdrRecordDate));
                txRecord.put("EVENT_START_TIME", sdfT.get().format(localStartDateTime));
                txRecord.put("EVENT_END_TIME", sdfT.get().format(localEndDateTime));
                txRecord.put("XDR_DATE", sdfT.get().format(localStartDateTime));
                txRecord.put("POPULATION_DATE", sdfT.get().format(new Date()));
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private Date computeLocalDateTime(Date cdrRecordDate, String utcOffsetCode) {
        int partnerOffsetInMins = 0;
        if (!utcOffsetCode.isEmpty()) {
            partnerOffsetInMins = ZoneOffset.of(partnerDateOffsets.get(utcOffsetCode)).getTotalSeconds() / 60;
        }
        int localOffsetInMins = ZoneOffset.of(localDateOffset).getTotalSeconds() / 60;

//        int offsetDiff = partnerOffsetInMins - localOffsetInMins;
        int offsetDiff =  localOffsetInMins - partnerOffsetInMins;
        Date date = DateUtility.addMinutesToDate(cdrRecordDate, offsetDiff);
        return date;
    }

    private String getSrvTypeKey(String msisdn, String imsi) {
        String srvTypeKey = "";
        if (cdrType.equalsIgnoreCase("tapin")) {
            String postpaid;
            if (!msisdn.isEmpty()) postpaid = transformationLib.getDimLookupCRMSubscriber(msisdn);
            else postpaid = transformationLib.getDimLookupCRMSubscriberByImsi(imsi);
            srvTypeKey = postpaid != null ? "5" : "6";
        } else {
            srvTypeKey = "3";
        }
        return srvTypeKey;
    }

    String srvTypeKey = "";
    public String getSrvTypeKey(String msisdn) {
        int flag = isPrepaid(msisdn);
        switch (flag) {
            case 0:
                srvTypeKey = "5";
                break;
            case 1:
                srvTypeKey = "6";
                break;
            case 3:
                srvTypeKey = "8";
                break;
            default:
                srvTypeKey = "-97";
                break;
        }
        return srvTypeKey;
    }

    public int isPrepaid(String msisdn) {
        String value;

        ReferenceDimSuscriberCRMInf subInfo = (ReferenceDimSuscriberCRMInf) cache.getRecord("DIM_CRM_INF_SUBSCRIBER_ALL_LOOKUP_CACHE", msisdn);
        if (subInfo != null) {
            value = subInfo.getPrepaidFlag();
            if (value != null)
                try {
                    int val =Integer.parseInt(value);
                    return val;
                } catch (Exception ignore) {
                    ignore.printStackTrace();
                }
        }
        return -1;// 0 for Prepaid, 1 for postpaid, -1 unknown
    }

    private Map<String, Object> handleBasicServiceUsed(Object data) {
        Map<String, Object> out = new HashMap<>();
        if (data != null) {
            List<Object> items = (List<Object>) data;

            String teleServiceCode = dsv(items, "TeleServiceCode");
            out.put("TeleServiceCode", teleServiceCode);

            Object infoList = getElementsList(items, "ChargeInformationList");
            List<Object> chargeInfoList = (List<Object>) infoList;
            for (Object  obj : chargeInfoList) {
                out.putAll(handleChargeInformation(obj));
            }
        }
        return out;
    }

    private Map<String, Object> handleChargeInformation(Object chargeInfoList) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (chargeInfoList != null) {
            List<Object> chargeInformation = (List<Object>) chargeInfoList;
            Object tag;
            tag = dsv(chargeInformation, "ChargedItem");
            out.put("ChargedItem", tag);
            tag = dsv(chargeInformation, "ExchangeRateCode");
            out.put("ExchangeRateCode", tag);

            List<Object> chargeDetail = getElementsList(chargeInformation, "ChargeDetailList");
//            if (chargeDetail != null && chargeDetail.size() == 1){
//                Object cdi = chargeDetail.get(0);
//                if (cdi instanceof List) chargeDetail = (List) cdi;
//            }

            if (chargeDetail.size() > 0) {
                List<Object> container = null;
                for (Object cdi : chargeDetail) {
                    if (container == null) {
                        container = (List) cdi;
                    } else {
                        container.addAll((List) cdi);
                    }
                }
                chargeDetail = container;
            }
            tag = dsv(chargeDetail, "ChargeType");
            out.put("ChargeType", tag);
            tag = sumReal(chargeDetail, "Charge");
            out.put("Charge", tag);
            tag = sumNum(chargeDetail, "ChargeableUnits");
            out.put("ChargeableUnits", tag);
            tag = sumNum(chargeDetail, "ChargedUnits");
            out.put("ChargedUnits", tag);
        }
        return out;
    }

    private List<Object> getElementsList(List<Object> chargeInformations, String name) {
        List<Object> cd = new ArrayList<>();
        if (chargeInformations != null) {
            for (Object ciEntry : chargeInformations) {
                List<Object> groups = (List<Object>) ciEntry;
                for (Object item : groups) {
                    LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) item;
                    if (map.containsKey(name)) {
                        cd.add(map.get(name));
                    }
                }
            }

//            List<List<Map<String, Object>>> l = chargeInformations.stream()
//                    .filter(e -> e.containsKey(name))
//                    .map(e -> (List<Map<String, Object>>) e.get(name))
//                    .collect(Collectors.toList());
//            for (List<Map<String, Object>> i : l) cd.addAll(i);
        }
        return cd;
    }


    private static void flatten(Object obj, LinkedHashMap<String, Object> records) {
        if (obj instanceof ArrayList) {
            for (LinkedHashMap<String, Object> rec : (ArrayList<LinkedHashMap<String, Object>>) obj)
                iterate(rec, records);

        } else {
            LinkedHashMap<String, Object> rec = (LinkedHashMap<String, Object>) obj;
            iterate(rec, records);
        }
    }

    private static void iterate(LinkedHashMap<String, Object> rec, LinkedHashMap<String, Object> records) {
        for (Map.Entry<String, Object> e : rec.entrySet()) {
            if (e.getValue() instanceof ArrayList) {
                flatten(e.getValue(), records);
            } else if (e.getValue() instanceof LinkedHashMap) {
                flatten(e.getValue(), records);
            } else {
                cummulate(records, e);
            }

        }
    }

    private Map<String, Object> getTapBasicInfo(MTAPRecordHeader header) {
        Map<String, Object> txRecord = new HashMap<>();
        if (cdrType == null) {
            if (sender == null) sender = header.getSender();
            if (recipient == null) recipient = header.getRecipient();
            cdrType = homePlmnCode.equalsIgnoreCase(sender) ? "TAPOUT" : "TAPIN";
            String partner = cdrType.equalsIgnoreCase("tapin") ? sender : recipient;
            partnerPlmnDetails = transformationLib.getDimTadigLookupByPLMN(partner);

        }
        txRecord.put("CDR_TYPE", cdrType);
        txRecord.put("sender", sender);
        txRecord.put("recipient", recipient);
        if (partnerPlmnDetails != null) {
            txRecord.put("partnerCountryName", partnerPlmnDetails.getCountry());
            txRecord.put("partnerCountryISO", partnerPlmnDetails.getIso3());
            txRecord.put("partnerOper", partnerPlmnDetails.getOperator());
        }
        return txRecord;
    }

    private static void cummulate(LinkedHashMap<String, Object> records, Map.Entry<String, Object> entry) {
        records.put(entry.getKey(), entry.getValue());
    }


    String dsv(List<Object> group, String key) {
        if (group == null) return "";
        List<String> distincts = new ArrayList<>();
        for (Object groupEntry : group) {
            List<Object> groups = (List<Object>) groupEntry;
            for (Object item : groups) {
                LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) item;
                if (map.containsKey(key)) {
                    distincts.add(String.valueOf(map.get(key)));
                }
            }
        }
        String c = StringUtils.join(distincts, "|");
        return c;
    }

    private Long sumNum(List<Object> group, String key) {
        long value = 0;
        for (Object ciEntry : group) {
            List<Object> groups = (List<Object>) ciEntry;
            for (Object item : groups) {
                LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) item;
                if (map.containsKey(key)) {
                    value += Long.parseLong(map.get(key).toString());
                }
            }
        }
        return value;
//        return group.stream()
//                .filter(e -> e.containsKey(key))
//                .map(e -> Long.parseLong(e.get(key).toString()))
//                .reduce(0L, (a, b) -> a + b);
    }

    Double sumReal(List<Object> group, String key) {
        double value = 0D;
        for (Object ciEntry : group) {
            List<Object> groups = (List<Object>) ciEntry;
            for (Object item : groups) {
                LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) item;
                if (map.containsKey(key)) {
                    value += Double.parseDouble(map.get(key).toString());
                }
            }
        }
        return value;

//        return group.stream()
//                .filter(e -> e.containsKey(key))
//                .map(e -> Double.parseDouble(e.get(key).toString()))
//                .reduce(0.0D, (a, b) -> a + b);
    }

    Object first(List<Map<String, Object>> group, String key) {
        if (group != null) {
            List<Object> l = group.stream()
                    .filter(e -> e.containsKey(key))
                    .map(e -> e.get(key))
                    .sorted().collect(Collectors.toList());
            if (l.size() > 0)
                return l.get(0);
        }
        return "";
    }

    Object last(List<Map<String, Object>> group, String key) {
        if (group != null) {
            List<Object> l = group.stream().map(e -> e.get(key))
                    .sorted().collect(Collectors.toList());
            if (l.size() > 0)
                return l.get(l.size() - 1);
        }
        return "";
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
        while ((st < len) && (val[st] == c)) {
            st++;
        }
//      while ((st < len) && (val[len - 1] <= 'c')) len--;
        return ((st > 0) || (len < s.length())) ? s.substring(st, len) : s;
    }

}

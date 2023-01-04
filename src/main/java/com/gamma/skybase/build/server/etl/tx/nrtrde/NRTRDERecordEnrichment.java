package com.gamma.skybase.build.server.etl.tx.nrtrde;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
//import com.gamma.telco.OpcoBusinessTransformation;
//import com.gamma.telco.utility.reference.ReferenceDimDialDigit;
import com.gamma.telco.utility.MTAPRecordHeader;
import com.gamma.telco.utility.reference.ReferenceDimTadigLookup;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by abhi on 09/28/2017
 */
public class NRTRDERecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(NRTRDERecordEnrichment.class);

    private OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private AppConfig appConfig = AppConfig.instance();
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private String homePlmnCode = appConfig.getProperty("app.datasource.plmn");
    private String localDateOffset = appConfig.getProperty("app.datasource.timeoffset");

    private static final String FORMAT3 = "yyyy-MM-dd HH:mm:ss";
    private static final ThreadLocal<SimpleDateFormat> sdfT2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT3));
    private String partnerDateOffset;
    private String cdrType;
    private String sender;
    private String recipient;
    private ReferenceDimTadigLookup partnerPlmnDetails;

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
        String fn = record.get("fileName").toString();
        txRecord.put("UNIV_FILE_NAME", fn);
        String na = "";
        if (fn.length() > 9) {
            na = fn.substring(2, 7);
            txRecord.put("NODE_ADDRESS", fn);

        }
        txRecord.put("eventType", record.get("eventType"));

        Long originalDuration = 0L;

        if (record.get("callEventDuration") != null)
            originalDuration = Long.parseLong(record.get("callEventDuration").toString());
        txRecord.put("ORIGINAL_DUR", originalDuration);

        txRecord.put("ZERO_DURATION_IND", originalDuration == 0 ? "1" : "0");

        if (record.get("callEventStartTimeStamp") != null) {
            handleTimestampTags(record, txRecord, originalDuration);
        }

        String eventType = record.get("eventType").toString().trim().toLowerCase();
        Object teleServiceCode = record.get("teleServiceCode");
        Object bearerServiceCode = record.get("bearerServiceCode");

        String basicServiceKey = null;
        if (bearerServiceCode != null)
            basicServiceKey = bearerServiceCode.toString();
        else if (teleServiceCode != null)
            basicServiceKey = teleServiceCode.toString();
//        record.put("BASIC_SERVICE_KEY", basicServiceKey);
        txRecord.put("BASIC_SERVICE_KEY", basicServiceKey);
        String causeForTermination = record.get("causeForTermination").toString();
        txRecord.put("CAUSEFORTERMINATION", causeForTermination);

        String ORIGINAL_B_NUM = "";
        Object otherMSISDN = null;
        String servedMSISDN = "";

        switch (eventType) {
            case "moc":
                otherMSISDN = record.get("connectedNumber");
                if (otherMSISDN == null)
                    otherMSISDN = record.get("dialledDigits");
                if (otherMSISDN != null)
                    otherMSISDN = normalizeOtherMSISDN(otherMSISDN.toString());
                ORIGINAL_B_NUM = otherMSISDN.toString();
                handleMOCMTC(record, txRecord);
                break;

            case "mtc":
                otherMSISDN = record.get("callingNumber");
                if (otherMSISDN != null)
                    otherMSISDN = normalizeOtherMSISDN(otherMSISDN.toString());
                ORIGINAL_B_NUM = otherMSISDN.toString();
                break;

            case "gprs":
                txRecord.putAll(record);
                handleGPRS(record, txRecord);
                break;

            default:
                logger.error(eventType + " event is not being handled!!");
                break;
        }

        txRecord.put("ORIGINAL_B_NUM", ORIGINAL_B_NUM);
        String flex_Ind_2 = "-99";
        String eventTypeKey = "";
        String eventDirectionKey = "";

        if (otherMSISDN != null && !otherMSISDN.toString().trim().isEmpty()) {
            txRecord.put("OTHER_MSISDN", otherMSISDN);
            ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(otherMSISDN.toString());
            if (ddk != null) {
                txRecord.put("OTHER_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                txRecord.put("OTHER_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                txRecord.put("EVENT_CATEGORY_KEY", ddk.getEventCategoryKey());
                txRecord.put("OTHER_MSISDN_ISO_CODE", ddk.getIsoCountryCode());
                txRecord.put("OTHER_OPER", ddk.getProviderDesc());

                txRecord.put("EVENT_TYPE_KEY", eventTypeKey);

                String cd = ddk.getTargetCountryDesc();
                String iso = ddk.getIsoCountryCode();

                switch (eventType) {
                    case "3":
                    case "4":
                        flex_Ind_2 = "6";
                        break;
                    case "2":
                        if (eventDirectionKey.equals("1"))
                            flex_Ind_2 = "9";
                        else if (eventDirectionKey.equals("2"))
                            flex_Ind_2 = "4";
                        break;
                    case "1":
                        if (cd.contains("249"))
                            flex_Ind_2 = "1";
                        else if (cd.contains("INMARSAT"))
                            flex_Ind_2 = "8";
                        else if (na.equalsIgnoreCase(iso))
                            flex_Ind_2 = "2";
                        else if (cd.contains("THURAYA"))
                            flex_Ind_2 = "7";
                        else
                            flex_Ind_2 = "3";
                        break;
                    default:
                        flex_Ind_2 = "0";
                }
            }
            txRecord.put("FLEXI_IND_2", flex_Ind_2);
        }

        ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN.toString());
        if (ddk != null) {
            txRecord.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
        }
        response.setResponseCode(true);
        response.setResponse(txRecord);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }

    private void handleMOCMTC(LinkedHashMap<String, Object> record, Map<String, Object> txRecord) {
        double billablePulse = 0L;
        double charge = 0D;
        String eventType = record.get("eventType").toString();
        String eventTypeKey = "";
        String eventDirectionKey = "";
        String eventStartTime = "";
        Long originalDuration = 0L;
        String otherMSISDN = "";
        String imsi = record.get("imsi").toString();

        String edrIdKey = eventTypeKey + '|' + eventDirectionKey + '|' + imsi + '|' + otherMSISDN
                + '|' + eventStartTime + '|' + originalDuration;
        txRecord.put("EDR_ID_KEY", edrIdKey);

        Object supplyMentaryServiceCode = record.get("supplyMentaryServiceCode") == null ? "" : record.get("supplyMentaryServiceCode");
        if (supplyMentaryServiceCode != null)
            txRecord.put("SUPPLEMENTARYSERVICECODE", supplyMentaryServiceCode.toString());

        Object thirdPartyNumber = record.get("thirdPartyNumber") == null ? "" : record.get("thirdPartyNumber");
        if (thirdPartyNumber != null) txRecord.put("THIRDPARTYNUMBER", thirdPartyNumber.toString());

        Object callReference = record.get("callReference") == null ? "" : record.get("callReference");
        if (callReference != null) txRecord.put("EVENT_REF_NUM", callReference.toString());

        txRecord.put("ZERO_BYTE_IND", "9");

        Object teleServiceCode = record.get("teleServiceCode");
        if (teleServiceCode != null) {
            switch (teleServiceCode.toString().trim()) {
                case "11"://voice mt
                case "12":
                    eventTypeKey = "1";
                    eventDirectionKey = eventType.equalsIgnoreCase("moc") ? "1" : "2";
                    break;

                case "20":
                case "21"://sms mt
                case "22":
                    eventTypeKey = "2";
                    eventDirectionKey = eventType.equalsIgnoreCase("mtc") ? "2" : "1";
                    billablePulse = 1L;
                    break;
            }
        }
        txRecord.put("BILLABLE_PULSE", billablePulse);
        txRecord.put("EVENT_TYPE_KEY", eventTypeKey);
        txRecord.put("EVENT_DIRECTION_KEY", eventDirectionKey);

        String chargeUnitIDKey = "-99";
        switch (eventTypeKey) {
            case "1":
                chargeUnitIDKey = "3";
                break;
            case "2":
                chargeUnitIDKey = "10";
                break;
            case "3":
            case "4":
                chargeUnitIDKey = "8";
                break;
        }
        txRecord.put("CHRG_UNIT_ID_KEY", chargeUnitIDKey);

        String zeroChargeInd = charge == 0 ? "1" : "0";
        txRecord.put("ZERO_CHRG_IND", zeroChargeInd);
    }

    private void handleGPRS(LinkedHashMap<String, Object> record, Map<String, Object> txRecord) {

        String eventTypeKey = "4";
        Double charge = 0D;

        Object dataVolumeIncoming = record.get("dataVolumeIncoming");
        long uploadVolume = 0L, downloadVolume = 0L;

        if (dataVolumeIncoming != null)
            uploadVolume = Long.parseLong(dataVolumeIncoming.toString().trim());

        Object dataVolumeOutgoing = record.get("dataVolumeOutgoing");

        if (dataVolumeOutgoing != null)
            downloadVolume = Long.parseLong(dataVolumeOutgoing.toString().trim());

        long totalVolume = downloadVolume + uploadVolume;

        txRecord.put("ORIGINAL_UPLINK_VOLUME", dataVolumeOutgoing);
        txRecord.put("ORIGINAL_DOWNLINK_VOLUME" ,dataVolumeIncoming);

        long billableBytes = (long) Math.ceil((double) totalVolume / 10240);
        txRecord.put("BILLABLE_BYTES", billableBytes);

        txRecord.put("EVENT_DIRECTION_KEY", "-99");
        txRecord.put("OTHER_MSISDN_NOP_ID_KEY", "-98");
        txRecord.put("OTHER_MSISDN_DIAL_DIGIT_KEY","-98");

        String ZERO_BYTE_IND = "0";
        if (billableBytes == 0)
            ZERO_BYTE_IND = "1";
        txRecord.put("ZERO_BYTE_IND", ZERO_BYTE_IND);

        String chargingId = record.get("chargingId") == null ? "" : record.get("chargingId").toString();
        txRecord.put("EVENT_REF_NUM", chargingId);

        String causeForTermination = record.get("causeForTermination") == null ? "" : record.get("causeForTermination").toString();
        txRecord.put("TERMINATION_REASON_KEY", causeForTermination);
        txRecord.put("SGSN_ADDRESS", record.get("sgsnAddress"));

        Object accessPointNameNI = record.get("accessPointNameNI");
        if (accessPointNameNI != null) {
            String apnNI = accessPointNameNI.toString().trim();
            if (apnNI.contains("mms")) eventTypeKey = "3";
            else eventTypeKey = "4";
        }
        txRecord.put("EVENT_TYPE_KEY", eventTypeKey);
        txRecord.put("EVENT_DIRECTION_KEY", "1");

        String zeroChargeInd = "0";
        if (record.get("chargeAmount") != null) {
            charge = Double.parseDouble(String.valueOf(record.get("chargeAmount")));
            zeroChargeInd = charge == 0 ? "1" : "0";
        }
        txRecord.put("CHARGE", charge);
        txRecord.put("ZERO_CHRG_IND", zeroChargeInd);

        String chargeUnitIDKey = "-99";
        switch (eventTypeKey) {
            case "1":
                chargeUnitIDKey = "3";
                break;
            case "2":
                chargeUnitIDKey = "10";
                break;
            case "3":
            case "4":
                chargeUnitIDKey = "8";
                break;
        }
        txRecord.put("CHRG_UNIT_ID_KEY", chargeUnitIDKey);

        String recEntityId = record.get("recEntityId") == null ? "" : record.get("recEntityId").toString();
        txRecord.put("recEntityId", recEntityId);
    }

    private void handleTimestampTags(LinkedHashMap<String, Object> record, Map<String, Object> txRecord, Long originalDuration) {
        Object cdrRecordTimeTxt = record.get("callEventStartTimeStamp");
        if (cdrRecordTimeTxt != null) {
            try {
                Date cdrRecordDate = sdfS.get().parse(cdrRecordTimeTxt.toString().trim());
//                Date localStartDateTime = computeLocalDateTime(cdrRecordDate);
                Date localStartDateTime = computeLocalDateTime(cdrRecordDate);
//                Date localEndDateTime = new Date(localStartDateTime.getTime() + originalDuration * 1000);
//                txRecord.put("CDR_RECORD_TIME", sdfT.get().format(cdrRecordDate));
                txRecord.put("EVENT_START_TIME", sdfT.get().format(cdrRecordDate));
                Date ed = new Date(cdrRecordDate.getTime() / 1000 + originalDuration);
                txRecord.put("EVENT_END_TIME", sdfT.get().format(ed));
                txRecord.put("XDR_DATE", sdfT.get().format(localStartDateTime));
                txRecord.put("POPULATION_DATE", sdfT.get().format(new Date()));
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private Date computeLocalDateTime(Date cdrRecordDate) {
        int partnerOffsetInMins = ZoneOffset.of(partnerDateOffset).getTotalSeconds() / 60;
        int localOffsetInMins = ZoneOffset.of(localDateOffset).getTotalSeconds() / 60;

        int offsetDiff = partnerOffsetInMins - localOffsetInMins;
        Date date = DateUtility.addMinutesToDate(cdrRecordDate, offsetDiff);
        return date;
    }

    private Map<String, Object> handleChargeInformation(Object chargeInfoList) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (chargeInfoList != null) {
            List<Map<String, Object>> chargeInformation = (List<Map<String, Object>>) chargeInfoList;
            Object tag;
            tag = dsv(chargeInformation, "ChargedItem");
            out.put("ChargedItem", tag);
            tag = dsv(chargeInformation, "ExchangeRateCode");
            out.put("ExchangeRateCode", tag);

            List<Map<String, Object>> chargeDetail = getElementsList(chargeInformation, "chargeDetail");
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
            if (e.getValue() instanceof ArrayList)
                flatten(e.getValue(), records);
            else if (e.getValue() instanceof LinkedHashMap)
                flatten(e.getValue(), records);
            else
                cummulate(records, e);
        }
    }

    private Map<String, Object> getNRTRDEBasicInfo(MTAPRecordHeader header) {
        Map<String, Object> txRecord = new HashMap<>();
        if (cdrType == null) {
            if (sender == null) sender = header.getSender();
            if (recipient == null) recipient = header.getRecipient();
            cdrType = homePlmnCode.equalsIgnoreCase(sender) ? "NRTRDEOUT" : "NRTRDEIN";
            String partner = cdrType.equalsIgnoreCase("nrtrdein") ? sender : recipient;
//            partnerPlmnDetails = transformationLib.getDimTadigLookupByPLMN(partner);

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

    private String getSrvTypeKey(String msisdn, String imsi) {
        String srvTypeKey = "";
        if (cdrType.equalsIgnoreCase("nrtrdein")) {
            String postpaid;
            if (!msisdn.isEmpty())
                postpaid = transformationLib.getDimLookupCRMSubscriber(msisdn);
            else
                postpaid = transformationLib.getDimLookupCRMSubscriberByImsi(imsi);
            srvTypeKey = postpaid != null ? "5" : "6";
        } else {
            srvTypeKey = "-99";
        }
        return srvTypeKey;
    }

    private static void cummulate(LinkedHashMap<String, Object> records, Map.Entry<String, Object> entry) {
        records.put(entry.getKey(), entry.getValue());
    }

    String dsv(List<Map<String, Object>> group, String key) {
        String c = group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString())
                .reduce("", (x, y) -> x + y + "|");
        if (c != null && c.length() > 1)
            c = c.substring(0, c.length() - 1);
        return c;
    }

    private Long sumNum(List<Map<String, Object>> group, String key) {
        return group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> Long.parseLong(e.get(key).toString()))
                .reduce(0L, (a, b) -> a + b);
    }

    Double sumReal(List<Map<String, Object>> group, String key) {
        return group.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> Double.parseDouble(e.get(key).toString()))
                .reduce(0.0D, (a, b) -> a + b);
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

    String distinctValueAsDSV(List<Map<String, Object>> coscs, String key) {
        List<Map<String, Object>> list = coscs.stream()
                .filter(p -> p.containsKey(key))
                .filter(distinct(p -> p.get(key)))
                .collect(Collectors.toList());
        return dsv(list, key);
    }

    private static <T> Predicate<T> distinct(Function<? super T, Object> key) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(key.apply(t), Boolean.TRUE) == null;
    }

    String normalizeServedMSISDN(String number) {
        if (number == null) return "0";
        number = ltrim(number, '0');
        if (number.startsWith("249")) return number;
        if (number.length() < 10)
            return number = "249" + number;
        return number;
    }

    String normalizeOtherMSISDN(String number) {
        if (number == null) return "0";
        number = ltrim(number, '0');
        if (number.startsWith("249")) return number;
        return number;
    }

    private String ltrim(String s, char c) {
        int len = s.length();
        int st = 0;
        char[] val = s.toCharArray();
        while ((st < len) && (val[st] == c)) st++;
//      while ((st < len) && (val[len - 1] <= 'c')) len--;
        return ((st > 0) || (len < s.length())) ? s.substring(st, len) : s;
    }
}
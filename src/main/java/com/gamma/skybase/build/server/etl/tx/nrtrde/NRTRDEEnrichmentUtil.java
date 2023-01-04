package com.gamma.skybase.build.server.etl.tx.nrtrde;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import com.gamma.telco.utility.reference.ReferenceDimTadigLookup;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class NRTRDEEnrichmentUtil {

    //    private static final Logger logger = LoggerFactory.getLogger(TxUtils.class);
    private final AppConfig appConfig = AppConfig.instance();
    private final String homePlmnCode = appConfig.getProperty("app.datasource.plmn");
//    private String localDateOffset = appConfig.getProperty("app.datasource.timeoffset");
//    private static final ThreadLocal<SimpleDateFormat> sdfT2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT3));
//    private static final String FORMAT3 = "yyyy-MM-dd HH:mm:ss";
//    private ReferenceDimTadigLookup partnerPlmnDetails;

    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private NRTRDEEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static NRTRDEEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new NRTRDEEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String fileName, nodeAddress;

    public Optional<String> getNodeAddress(String field) {
        fileName = getValue(field);
        if (fileName != null && fileName.length() > 9)
            nodeAddress = fileName.substring(2, 7);
        if (nodeAddress != null) return Optional.of(nodeAddress);
        return Optional.empty();
    }

    Long callDuration;

    public Optional<String> getCallDuration(String field) {
        String s = getValue(field);
        callDuration = 0L;
        try {
            if (s != null)
                callDuration = Long.parseLong(s);
            return Optional.of(String.valueOf(callDuration));
        } catch (NumberFormatException e) { // Ignore non numbers
        }
        return Optional.empty();
    }

    Date callStartTime;
    String genFullDate;
    public Optional<String> getStartTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callStartTime = sdfS.get().parse(s);
                genFullDate = fullDate.get().format(callStartTime);
                return Optional.of(sdfT.get().format(callStartTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    Date callEndTime;

    public Optional<String> getEndTime(String time, String callEventDuration) {
        if (callDuration == null) getCallDuration(callEventDuration);
        if (callStartTime == null) getStartTime(time);

        if (callStartTime == null || callDuration == null) return Optional.empty();

        callEndTime = new Date(callStartTime.getTime() / 1000 + callDuration);
        return Optional.of(sdfT.get().format(callEndTime));
    }

    String bearerServiceCode, teleServiceCode, basicServiceKey;

    public Optional<String> getBasicServiceKey(String bearer, String tele) {
        bearerServiceCode = getValue(bearer);
        teleServiceCode = getValue(tele);
        if (bearerServiceCode != null) basicServiceKey = bearerServiceCode;
        else if (teleServiceCode != null) basicServiceKey = teleServiceCode;
        if (basicServiceKey != null) return Optional.of(basicServiceKey);
        return Optional.empty();
    }

    String eventType, eventTypeKey, eventDirectionKey;

    String connectedNumber, dialledDigits, origBNum;
    String otherMSISDN, callingNumber;
    long billablePulse = 0L;

    public Optional<String> getServType() {
        if (eventType == null) eventType = getValue("eventType");
        if (eventType != null)
            switch (eventType) {
                case "moc":
                    connectedNumber = getValue("connectedNumber");
                    dialledDigits = getValue("dialledDigits");
                    if (connectedNumber != null)
                        origBNum = connectedNumber;
                    else if (dialledDigits != null)
                        origBNum = dialledDigits;
                    otherMSISDN = normalizeMSISDN(origBNum);
                    if (teleServiceCode != null)
                        teleServiceCode = getValue("teleServiceCode");
                    if (teleServiceCode != null)
                        switch (teleServiceCode) {
                            case "11"://voice mt
                            case "12":
                                eventDirectionKey = "moc".equalsIgnoreCase(eventType) ? "1" : "2";
                                break;
                            case "20":
                            case "21"://sms mt
                            case "22":
                                eventDirectionKey = "mtc".equalsIgnoreCase(eventType) ? "2" : "1";
                                break;
                        }
                    break;

                case "mtc":
                    callingNumber = getValue("callingNumber");
                    origBNum = callingNumber;
                    otherMSISDN = normalizeMSISDN(origBNum);
                    if (teleServiceCode != null)
                        teleServiceCode = getValue("teleServiceCode");
                    if (teleServiceCode != null)
                        switch (teleServiceCode) {
                            case "11"://voice mt
                            case "12":
                                eventDirectionKey = "moc".equalsIgnoreCase(eventType) ? "1" : "2";
                                break;
                            case "20":
                            case "21"://sms mt
                            case "22":
                                eventDirectionKey = "mtc".equalsIgnoreCase(eventType) ? "2" : "1";
                                billablePulse = 1L;
                                break;
                        }
                    break;

//                case "gprs":
//                    String accessPointNameNI = getValue("accessPointNameNI");
//                    if (accessPointNameNI != null)
//                        if (accessPointNameNI.contains("mms"))
//                            eventTypeKey = "3";
//                        else eventTypeKey = "4";
//                    eventDirectionKey = "1";

            }
        return Optional.empty();
    }

    public Optional<String> getOtherMSISDN() {
        if (otherMSISDN == null) getServType();

        if (otherMSISDN != null) return Optional.of(otherMSISDN);
        return Optional.empty();
    }

    public Optional<Long> getBillablePulse(String f){
        String originalDuration = getValue(f);
        if(originalDuration != null) {
            try {
                long v = Long.parseLong(originalDuration);
                billablePulse = (long) Math.ceil(v / 60);
                return Optional.of(billablePulse);
            }
            catch (Exception ignore){}
        }
        return Optional.empty();
    }

    public Optional<String> getOriginalBNumber() {
        if (origBNum == null)
            getServType();
//        System.out.println("\n\nORIGINAL_B_NUM:   " + origBNum);
        if (origBNum != null) return Optional.of(origBNum);
        return Optional.empty();
    }

    public Optional<String> getEventTypeKey() {
        if (teleServiceCode != null)
            teleServiceCode = getValue("teleServiceCode");
        if (teleServiceCode != null)
            switch (teleServiceCode) {
                case "20":
                case "21":
                    eventTypeKey = "2";
                    break;
                default:
                    eventTypeKey="1";
            }
        if (eventTypeKey != null) return Optional.of(eventTypeKey);
        return Optional.empty();
    }

    public Optional<String> getDirectionKey() {
        if (eventDirectionKey == null) getServType();

        if (eventDirectionKey != null) return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    String dataVolumeIncoming;
    Long downloadVolume;

    public Optional<String> getDownloadVolume() {
        dataVolumeIncoming = getValue("dataVolumeIncoming");
        if (dataVolumeIncoming != null)
            try {
                downloadVolume = Long.parseLong(dataVolumeIncoming);
                return Optional.of(String.valueOf(downloadVolume));
            } catch (NumberFormatException ignored) {
            }
        return Optional.empty();
    }

    String dataVolumeOutgoing;
    Long uploadVolume;

    public Optional<String> getUploadVolume() {
        dataVolumeOutgoing = getValue("dataVolumeOutgoing");
        if (dataVolumeOutgoing != null)
            try {
                uploadVolume = Long.parseLong(dataVolumeOutgoing);
                return Optional.of(String.valueOf(uploadVolume));
            } catch (NumberFormatException ignored) {
            }
        return Optional.empty();
    }
    long totalVolume = 0L;

    public Optional<Long> getBillableBytes() {
        if (downloadVolume != null && uploadVolume != null) {
            totalVolume = downloadVolume + uploadVolume;
        }
        Double bytes = Math.ceil((double) totalVolume / 10240);
        return Optional.of(bytes.longValue());
    }

    String chargeAmount;

    public Optional<String> getCharge(String chargeAmnt) {
        chargeAmount = getValue(chargeAmnt);
        if (chargeAmount != null) {
            try {
                Double.parseDouble(chargeAmount);
                return Optional.of(eventTypeKey);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    public Optional<String> getChargeUnitIDKey() {
        if (eventTypeKey == null) getServType();
        String chargeUnitIDKey = null;
        if (eventTypeKey != null)
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
        if (chargeUnitIDKey != null) return Optional.of(chargeUnitIDKey);
        return Optional.empty();
    }

    public Optional<String> getFlexInd2(String cd, String iso) {
        if (eventTypeKey == null) getServType();
        String flex_Ind_2 = null;
        switch (eventType) {
            case "1":
                if (cd.contains("249"))
                    flex_Ind_2 = "1";
                else if (cd.contains("INMARSAT"))
                    flex_Ind_2 = "8";
                else if (nodeAddress.equalsIgnoreCase(iso))
                    flex_Ind_2 = "2";
                else if (cd.contains("THURAYA"))
                    flex_Ind_2 = "7";
                else
                    flex_Ind_2 = "3";
                break;
            case "2":
                if (eventDirectionKey.equals("1"))
                    flex_Ind_2 = "9";
                else if (eventDirectionKey.equals("2"))
                    flex_Ind_2 = "4";
                break;
            case "3":
            case "4":
                flex_Ind_2 = "6";
                break;
            default:
                flex_Ind_2 = "0";
        }
        if (flex_Ind_2 != null) return Optional.of(flex_Ind_2);
        return Optional.empty();
    }

    public String getEDRIdKey() {
        return eventTypeKey + '|' + eventDirectionKey + '|' + getValue("imsi") + '|' + otherMSISDN
                + '|' + callStartTime + '|' + callDuration;
    }

    String normalizeMSISDN(String number) {
        if (number == null)
            return "0";
        number = ltrim(number, '0');
        if (number.startsWith("249"))
            return number;
        if (number.length() < 10)
            number = "249" + number;
        return number;
    }

    private String cdrType, sender, recipient;

    public Map<String, Object> getNRTRDEBasicInfo() {
        Map<String, Object> values = new HashMap<>();
        ReferenceDimTadigLookup partnerPlmnDetails = null;
        if (cdrType == null) {
            if (sender == null)
                sender = getValue("sender");
            if (recipient == null)
                recipient = getValue("recipient");
            cdrType = homePlmnCode.equalsIgnoreCase(sender) ? "NRTRDEOUT" : "NRTRDEIN";
            String partner = cdrType.equalsIgnoreCase("nrtrdein") ? sender : recipient;
            partnerPlmnDetails = txLib.getDimTadigLookupByPLMN(partner);

        }
        values.put("CDR_TYPE", cdrType);
        if (partnerPlmnDetails != null) {
            values.put("partnerCountryName", partnerPlmnDetails.getCountry());
            values.put("partnerCountryISO", partnerPlmnDetails.getIso3());
            values.put("partnerOper", partnerPlmnDetails.getOperator());
        }
        return values;
    }

    String srvTypeKey = null;

    Optional<String> getSrvTypeKey() {
        if (cdrType == null) getNRTRDEBasicInfo();
        String msisdn = getValue("msisdn");
        String imsi = getValue("imsi");
        if (cdrType.equals("nrtrdein")) {
            String postpaid;
            if (!msisdn.isEmpty())
                postpaid = txLib.getDimLookupCRMSubscriber(msisdn);
            else
                postpaid = txLib.getDimLookupCRMSubscriberByImsi(imsi);
            srvTypeKey = postpaid != null ? "5" : "6";
            return Optional.of(srvTypeKey);
        }
        return Optional.empty();
    }

//    ReferenceDimTadigLookup partnerPlmnDetails;
//    private Date computeLocalDateTime(Date cdrRecordDate) {
//        int partnerOffsetInMins = ZoneOffset.of(partnerDateOffset).getTotalSeconds() / 60;
//        int localOffsetInMins = ZoneOffset.of(localDateOffset).getTotalSeconds() / 60;
//
//        int offsetDiff = partnerOffsetInMins - localOffsetInMins;
//        Date date = DateUtility.addMinutesToDate(cdrRecordDate, offsetDiff);
//        return date;
//    }

    private String ltrim(String s, char c) {
        int len = s.length();
        int st = 0;
        char[] val = s.toCharArray();
        while ((st < len) && (val[st] == c)) st++;
//      while ((st < len) && (val[len - 1] <= 'c')) len--;
        return st > 0 ? s.substring(st, len) : s;
    }

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }

//    public String getChrgUnitIdKey(String eventTypeKey) {
//        return txLib.getChrgUnitIdKey(eventTypeKey);
//    }

//    public String getSdrValue(Date cdrDate) {
//        return txLib.getSdrValue(cdrDate);
//    }

//    public ReferenceDimTadigLookup getDimTadigLookupByPLMN(String partner) {
//        return txLib.getDimTadigLookupByPLMN(partner);
//    }

//    private Map<String, Object> handleChargeInformation(Object chargeInfoList) {
//        Map<String, Object> out = new LinkedHashMap<>();
//        if (chargeInfoList != null) {
//            List<Map<String, Object>> chargeInformation = (List<Map<String, Object>>) chargeInfoList;
//            Object tag;
//            tag = dsv(chargeInformation, "ChargedItem");
//            out.put("ChargedItem", tag);
//            tag = dsv(chargeInformation, "ExchangeRateCode");
//            out.put("ExchangeRateCode", tag);
//
//            List<Map<String, Object>> chargeDetail = getElementsList(chargeInformation, "chargeDetail");
//            tag = dsv(chargeDetail, "ChargeType");
//            out.put("ChargeType", tag);
//            tag = sumReal(chargeDetail, "Charge");
//            out.put("Charge", tag);
//            tag = sumNum(chargeDetail, "ChargeableUnits");
//            out.put("ChargeableUnits", tag);
//            tag = sumNum(chargeDetail, "ChargedUnits");
//            out.put("ChargedUnits", tag);
//        }
//        return out;
//    }

//    private static void flatten(Object obj, LinkedHashMap<String, Object> records) {
//        if (obj instanceof ArrayList) {
//            for (LinkedHashMap<String, Object> rec : (ArrayList<LinkedHashMap<String, Object>>) obj)
//                iterate(rec, records);
//
//        } else {
//            LinkedHashMap<String, Object> rec = (LinkedHashMap<String, Object>) obj;
//            iterate(rec, records);
//        }
//    }

//    private static void iterate(LinkedHashMap<String, Object> rec, LinkedHashMap<String, Object> records) {
//        for (Map.Entry<String, Object> e : rec.entrySet()) {
//            if (e.getValue() instanceof ArrayList)
//                flatten(e.getValue(), records);
//            else if (e.getValue() instanceof LinkedHashMap)
//                flatten(e.getValue(), records);
//            else
//                cummulate(records, e);
//        }
//    }

//    private static void cummulate(LinkedHashMap<String, Object> records, Map.Entry<String, Object> entry) {
//        records.put(entry.getKey(), entry.getValue());
//    }

//    String dsv(List<Map<String, Object>> group, String key) {
//        String c = group.stream()
//                .filter(e -> e.containsKey(key))
//                .map(e -> e.get(key).toString())
//                .reduce("", (x, y) -> x + y + "|");
//        if (c != null && c.length() > 1)
//            c = c.substring(0, c.length() - 1);
//        return c;
//    }

//    private Long sumNum(List<Map<String, Object>> group, String key) {
//        return group.stream()
//                .filter(e -> e.containsKey(key))
//                .map(e -> Long.parseLong(e.get(key).toString()))
//                .reduce(0L, (a, b) -> a + b);
//    }

//    Double sumReal(List<Map<String, Object>> group, String key) {
//        return group.stream()
//                .filter(e -> e.containsKey(key))
//                .map(e -> Double.parseDouble(e.get(key).toString()))
//                .reduce(0.0D, (a, b) -> a + b);
//    }

//    Object first(List<Map<String, Object>> group, String key) {
//        if (group != null) {
//            List<Object> l = group.stream()
//                    .filter(e -> e.containsKey(key))
//                    .map(e -> e.get(key))
//                    .sorted().collect(Collectors.toList());
//            if (l.size() > 0)
//                return l.get(0);
//        }
//        return "";
//    }

//    Object last(List<Map<String, Object>> group, String key) {
//        if (group != null) {
//            List<Object> l = group.stream().map(e -> e.get(key))
//                    .sorted().collect(Collectors.toList());
//            if (l.size() > 0)
//                return l.get(l.size() - 1);
//        }
//        return "";
//    }

//    String distinctValueAsDSV(List<Map<String, Object>> coscs, String key) {
//        List<Map<String, Object>> list = coscs.stream()
//                .filter(p -> p.containsKey(key))
//                .filter(distinct(p -> p.get(key)))
//                .collect(Collectors.toList());
//        return dsv(list, key);
//    }

//    private static <T> Predicate<T> distinct(Function<? super T, Object> key) {
//        Map<Object, Boolean> map = new ConcurrentHashMap<>();
//        return t -> map.putIfAbsent(key.apply(t), Boolean.TRUE) == null;
//    }
}
package com.gamma.skybase.build.server.etl.decoder.ggsn;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

/**
 * Created by abhi on 19/08/21
 */
public class GGSNRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(GGSNRecordEnrichment.class);
    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    ThreadLocal<SimpleDateFormat> sdfT1 = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyMMddHHmmss Z"));
    String opcoCode = AppConfig.instance().getProperty("app.datasource.opcode");
    String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    Object serviceList = null;

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> transformedData = new LinkedHashMap<>();
        Map<String, Map<String, Object>> records = new HashMap<>();
        if (data.containsKey("listOfServiceData")) {
            serviceList = data.remove("listOfServiceData");
            Map<String, SortedSet<Map<String, Object>>> groupOfRatingGroups = handleListOfServiceData(serviceList);

            groupOfRatingGroups.forEach((key, val) -> {
                if (val.size() > 0) {
                    String timeOfFirstUsage = val.first().get("timeOfFirstUsage").toString();
                    String timeOfLastUsage = val.last().get("timeOfLastUsage").toString();

                    AtomicLong upSum = new AtomicLong(0L);
                    AtomicLong downSum = new AtomicLong(0L);
                    long totalVolume;
                    val.forEach(e -> {
                        if (e.get("datavolumeFBCUplink") != null)
                            upSum.addAndGet(Long.parseLong(e.get("datavolumeFBCUplink").toString()));
                        if (e.get("datavolumeFBCDownlink") != null)
                            downSum.addAndGet(Long.parseLong((e.get("datavolumeFBCDownlink").toString())));
                    });
                    totalVolume = upSum.get() + downSum.get();

                    Map<String, Object> mapToForm = val.last();
                    mapToForm.put("DATA_VOLUME_FBC_UPLINK", upSum);
                    mapToForm.put("DATA_VOLUME_FBC_DOWNLINK", downSum);
                    mapToForm.put("TOTAL_VOLUME", totalVolume);
                    mapToForm.put("RATING_GROUP", mapToForm.get("ratingGroup"));
                    mapToForm.put("RG_SEQUENCE_NUM", mapToForm.get("localSequenceNumber"));
                    mapToForm.put("SERVICE_COND_CHANGE", mapToForm.get("serviceConditionChange"));
                    mapToForm.put("TIME_FIRST_USAGE", getString(timeOfFirstUsage));
                    mapToForm.put("TIME_LAST_USAGE", getString(timeOfLastUsage));
                    mapToForm.put("REPORT_TIME", getString(mapToForm.get("timeOfReport").toString()));
                    mapToForm.put("SGSN_ADDRESS", extractIpAddress(mapToForm.get("sgsnAddress")));
                    float billableVolume = (float) totalVolume / 1024;
                    mapToForm.put("BILLABLE_VOLUME", ((Double) Math.ceil(billableVolume)).intValue());

                    if (mapToForm.get("qoSInformationNeg") != null) {
                        ArrayList<LinkedHashMap<String,Object>> qosInfoValue = (ArrayList<LinkedHashMap<String, Object>>) mapToForm.get("qoSInformationNeg");
                        //if size > 0

                        Object qCI = qosInfoValue.get(0).get("qCI");
                        mapToForm.put("QOS_INFO_qCI", qCI);

                        Object aRP = qosInfoValue.get(0).get("aRP");
                        mapToForm.put("QOS_INFO_aRP", aRP);
                    }
                    records.put(key, mapToForm);
                }
            });
        }

        records.forEach((key, rg) -> {
            rg.put("EVENT_TYPE", data.get("eventType"));
            rg.put("RECORD_TYPE", data.get("recordType"));
            rg.put("SERVED_IMSI", data.get("servedIMSI"));
            String strpGWAddress = null, strservingNodeAddress = null, strservedPDPPDNAddress = null;
            if (data.get("PGWAddress") != null) {
                String pGW = data.get("PGWAddress").toString();
                strpGWAddress = pGW.substring(pGW.indexOf('=') + 1, pGW.indexOf('}'));
            }
            rg.put("PGW_ADDRESS", strpGWAddress);

            if (data.get("ServingNodeAddress") != null) {
                Object servingNodeAddress = data.get("ServingNodeAddress");
                String servingNode = servingNodeAddress.toString();
                strservingNodeAddress = servingNode.substring(servingNode.indexOf('=') + 1, servingNode.indexOf('}'));
            }
            rg.put("SGSN_ADDRESS", strservingNodeAddress);

            if (data.get("servedPDPPDNAddress") != null) {
                Object servedPDPPDNAddress = data.get("servedPDPPDNAddress");
                String servedPDPPDN = servedPDPPDNAddress.toString();

                String served = servedPDPPDN.substring(servedPDPPDN.indexOf('=') + 1, servedPDPPDN.indexOf('}'));
                String[] servedPdpdNAddress = served.split("=");
                strservedPDPPDNAddress = servedPdpdNAddress[1];
            }
            rg.put("PDP_ADDRESS", strservedPDPPDNAddress);
            rg.put("CHARGING_ID", data.get("chargingID"));
            Object accessPointNameNI = data.get("accessPointNameNI");
            rg.put("APN_NAME", accessPointNameNI);
            if (accessPointNameNI != null) {
                String eventTypeKey;
                if (accessPointNameNI.toString().contains("mms")) eventTypeKey = "3";
                else eventTypeKey = "4";
                rg.put("EVENT_TYPE_KEY", eventTypeKey);
            }
            rg.put("PDP_TYPE", data.get("pdpPDNType"));
            rg.put("DYNAMIC_ADDRESS_FLAG", data.get("dynamicAddressFlag"));

            Object recordOpeningTime = data.get("recordOpeningTime");
            if (recordOpeningTime != null) {
                Date eventStartTime = null;
                try {
                    eventStartTime = sdfS.get().parse(recordOpeningTime.toString());
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                String eventStart = sdfT.get().format(eventStartTime);
                rg.put("EVENT_START_TIME", eventStart);
                rg.put("XDR_DATE", eventStart);
                rg.put("POPULATION_DATE", sdfT.get().format(new Date()));
                String eventDate = fullDate.get().format(eventStartTime);
                rg.put("EVENT_DATE", eventDate); //31
                rg.put("ORIGINAL_DUR", data.get("duration"));
                Object causeForRecClosing = data.get("causeForRecClosing");
                Object recordSequenceNumber = data.get("recordSequenceNumber");

                String recTypeIdKey;
                if (recordSequenceNumber == null)
                    recTypeIdKey = "4";
                else if (recordSequenceNumber.toString().equals("1"))
                    recTypeIdKey = "5";
                else {
                    if (causeForRecClosing.toString().equals("0") || causeForRecClosing.toString().equals("1"))
                        recTypeIdKey = "7";
                    else
                        recTypeIdKey = "6";
                }
                rg.put("recordSequenceNumber", recordSequenceNumber);
                rg.put("causeForRecClosing", causeForRecClosing);
                rg.put("REC_TYPE_ID_KEY", recTypeIdKey);
                rg.put("NODE_ID", data.get("nodeID"));
                rg.put("EXT_TYPE", data.get("extensionType"));
                Object servedMSISDN = data.get("servedMSISDN");
                if (servedMSISDN != null) {
                    rg.put("ORIGINAL_A_NUM", servedMSISDN);
                    String servedMsisdn = normalizeMSISDN((String) servedMSISDN);
                    rg.put("SERVED_MSISDN", servedMsisdn);
                    ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN.toString());
                    if (ddk != null) {
                        rg.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                        rg.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                    }
                }
                Object chargingCharacteristics = data.get("chargingCharacteristics");
                Object servingNodePLMNIdentifier = data.get("servingNodePLMNIdentifier");

                if (chargingCharacteristics != null) {
                    chargingCharacteristics = ltrim(chargingCharacteristics.toString(), '0').trim();
                    boolean hasPLMN = hasPLMN(servingNodePLMNIdentifier.toString());
                    String srvTypeKey;
                    switch (chargingCharacteristics.toString().trim()) {
                        case "4":
                            srvTypeKey = hasPLMN ? "1" : "5";
                            break;
                        case "8":
                            srvTypeKey = hasPLMN ? "2" : "6";
                            break;
                        default:
                            srvTypeKey = "-99";
                            break;
                    }
                    rg.put("SRV_TYPE_KEY", srvTypeKey);
                    rg.put("chargingCharacteristics", chargingCharacteristics);
                }
                rg.put("SERVING_PLMN_ID", servingNodePLMNIdentifier);
                Object servedIMEISV = data.get("servedIMEISV");
                rg.put("SERVED_IMEI", servedIMEISV);
                if (servedIMEISV != null && !servedIMEISV.toString().isEmpty())
                    rg.put("tac", servedIMEISV.toString().substring(0, 8));
                rg.put("RAT_TYPE", data.get("rATType"));
                Object userLocationInformation = data.get("userLocationInformation");
                rg.put("CGI_ID", userLocationInformation);

                String userLoc = String.valueOf(extractExtentedCellId(userLocationInformation));
                rg.put("ECI", userLoc);
                Object servingNodeType = data.get("ServingNodeType");
                String strServingNode = servingNodeType.toString();
                String strServingNodeType = strServingNode.substring(strServingNode.indexOf('=') + 1, strServingNode.indexOf('}'));
                rg.put("SERVING_NODE_TYPE", strServingNodeType);
                rg.put("CDR_SEQUENCE_NUM", data.get("localSequenceNumber"));
                Object seqNumber = data.get("_SEQUENCE_NUMBER");
                rg.put("_SEQUENCE_NUMBER", seqNumber);
                rg.put("FILE_NAME", data.get("fileName"));
            }
            records.put(key, rg);
        });
        records.forEach((key, value) -> transformedData.put(key, (Object) value));
        return transformedData;
    }

    public static String extractExtentedCellId(Object userlocHex) {
        String cellId = "";
        if (userlocHex != null) {
            String hexValue = String.valueOf(userlocHex);
            if (hexValue.length() > 0) {
                // extract last 4 byte or 8 hex characters from user location information
                String hexCellLoc = StringUtils.right(hexValue, 8);
                BigInteger bi = new BigInteger(hexCellLoc, 16);
                cellId = String.valueOf(bi.longValue());
            }
        }
        return cellId;
//        String last8Characters = userlocHex.substring(userlocHex.length() - 8);
//        long number = Long.parseLong(last8Characters, 16);
//        return number;
    }

    boolean hasPLMN(String plmn) {
        if (plmn == null || plmn.trim().isEmpty()) return false;
        String[] opcoCodes = opcoCode.split(",");
        for (int i = 0; i < opcoCodes.length; i++)
            if (plmn.startsWith(opcoCodes[i])) return true;
        return false;
    }

    @Override
    @SuppressWarnings("Duplicates")
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else response.setResponseCode(false);
        return response;
    }

    private String extractIpAddress(Object addressList) {
        String extract;
        if (addressList instanceof List) {
            Set<String> ips = new HashSet<>((ArrayList<String>) addressList);
            extract = StringUtils.join(ips, "|");
        } else {
            extract = String.valueOf(addressList);
        }
        return extract;
    }

    private Map<String, SortedSet<Map<String, Object>>> handleListOfServiceData(Object serviceData) {
        if (serviceData instanceof ArrayList) {
            ArrayList<LinkedHashMap<String, Object>> sd = (ArrayList<LinkedHashMap<String, java.lang.Object>>) serviceData;
            LinkedHashMap<String, Object> csc = sd.get(0);
            ArrayList<LinkedHashMap<String, Object>> list = (ArrayList<LinkedHashMap<String, Object>>) csc.get("ChangeOfServiceCondition");
            Map<String, SortedSet<Map<String, Object>>> groupedMaps = list.stream().collect(
                    Collectors.groupingBy(
                            map -> {
                                return map.get("ratingGroup") + "|" + map.get("localSequenceNumber").toString();
                            },
                            Collector.of(
                                    () -> new TreeSet<>(Comparator.comparing(m -> {
                                        return m.get("timeOfLastUsage").toString();
                                    })),
                                    Set::add,
                                    (left, right) -> {
                                        left.addAll(right);
                                        return left;
                                    }
                            )
                    )
            );
            return groupedMaps;
        }
        return null;
    }

    private static String getString(String dates) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyMMddHHmmss Z");
        LocalDateTime dateTime = LocalDateTime.parse(dates, inputFormatter);
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
        return dateTime.format(outputFormatter);
    }

    String normalizeMSISDN(String number) {
        if (number != null) {
            if (number.startsWith("0")) {
                number = ltrim(number, '0');
            }
            if (number.length() < 10) {
                if (number.startsWith("966")){
                    return number;
                }
                else {
                    number = "966" + number;
                }
            }
            return number;
        }
        return "";
    }
}

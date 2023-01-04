package com.gamma.skybase.build.server.etl.tx.gsn;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

/**
 * Created by abhi on 19/08/21
 */
public class GGSNRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(GGSNRecordEnrichment.class);
    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private static final String FORMAT1 = "yyMMddHHmmss";
    private static final String FORMAT2 = "yyyyMMdd HH:mm:ss";

    private static final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT1));
    private static final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT2));

    String opcoCode = AppConfig.instance().getProperty("app.datasource.opcode");
    String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> commonAttributes = new LinkedHashMap<>();
        try {
            Object eventType = data.get("eventType");
            Object recordType = data.get("recordType");
            Object servedIMSI = data.get("servedIMSI");
            Object pGWAddress = data.get("pGWAddress");
            Object servingNodeAddress = data.get("servingNodeAddress");
            Object servedPDPPDNAddress = data.get("servedPDPPDNAddress");
            Object chargingID = data.get("chargingID");
            Object accessPointNameNI = data.get("accessPointNameNI");
            Object pdpPDNType = data.get("pdpPDNType");
            Object dynamicAddressFlag = data.get("dynamicAddressFlag");
            Object recordOpeningTime = data.get("recordOpeningTime");
            Object duration = data.get("duration");
            Object causeForRecClosing = data.get("causeForRecClosing");
            Object recordSequenceNumber = data.get("recordSequenceNumber");
            Object nodeID = data.get("nodeID");
            Object extensionType = data.get("extensionType");
            Object servedMSISDN = data.get("servedMSISDN");
            Object chargingCharacteristics = data.get("chargingCharacteristics");
            Object servingNodePLMNIdentifier = data.get("servingNodePLMNIdentifier");
            Object servedIMEISV = data.get("servedIMEISV");
            Object rATType = data.get("rATType");
            Object userLocationInformation = data.get("userLocationInformation");
            Object servingNodeType = data.get("servingNodeType");
            Object fileName = data.get("fileName");

            commonAttributes.put("EVENT_TYPE", eventType);
            commonAttributes.put("RECORD_TYPE", recordType);
            commonAttributes.put("SERVED_IMSI", servedIMSI);
            commonAttributes.put("SERVED_IMEI", servedIMEISV);
            if (servedIMEISV != null && !servedIMEISV.toString().isEmpty())
                commonAttributes.put("tac", servedIMEISV.toString().substring(0, 8));

            commonAttributes.put("PGW_ADDRESS", extractIpAddress(pGWAddress));
            commonAttributes.put("SGSN_ADDRESS", extractIpAddress(servingNodeAddress));
            commonAttributes.put("PDP_ADDRESS", extractIpAddress(servedPDPPDNAddress));
            commonAttributes.put("CHARGING_ID", chargingID);
            commonAttributes.put("APN_NAME", accessPointNameNI);
            commonAttributes.put("PDP_TYPE", pdpPDNType);
            commonAttributes.put("DYNAMIC_ADDRESS_FLAG", dynamicAddressFlag);
            commonAttributes.put("ORIGINAL_DUR", duration);

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
            commonAttributes.put("REC_TYPE_ID_KEY", recTypeIdKey);

            if (accessPointNameNI != null) {
                String eventTypeKey;
                if (accessPointNameNI.toString().contains("mms")) eventTypeKey = "3";
                else eventTypeKey = "4";
                commonAttributes.put("EVENT_TYPE_KEY", eventTypeKey);
            }

            if (recordOpeningTime != null) {
                Date eventStartTime = sdfS.get().parse(recordOpeningTime.toString());
                commonAttributes.put("EVENT_START_TIME", sdfT.get().format(eventStartTime)); // 2
                commonAttributes.put("XDR_DATE", sdfT.get().format(eventStartTime)); // 2
                commonAttributes.put("POPULATION_DATE_TIME", sdfT.get().format(new Date())); // 2
                commonAttributes.put("GENERATED_FULL_DATE", new SimpleDateFormat("yyyyMMdd").format(eventStartTime) + " 00:00:00"); //31
            }

            commonAttributes.put("FILE_NAME", fileName);
            commonAttributes.put("SERVING_PLMN_ID", servingNodePLMNIdentifier);
            commonAttributes.put("SERVING_NODE_TYPE", servingNodeType);
            commonAttributes.put("CGI_ID", userLocationInformation);
            commonAttributes.put("NODE_ID", nodeID);
            commonAttributes.put("EXT_TYPE", extensionType);
            commonAttributes.put("RAT_TYPE", rATType);

            if (chargingCharacteristics != null) {
                chargingCharacteristics = ltrim(chargingCharacteristics.toString(), '0').trim();
                boolean hasPLMN = servingNodePLMNIdentifier != null && servingNodePLMNIdentifier.toString().startsWith(opcoCode);
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
                commonAttributes.put("SRV_TYPE_KEY",  srvTypeKey);
                commonAttributes.put("chargingCharacteristics",  chargingCharacteristics);
            }

            if (servedMSISDN != null) {
                commonAttributes.put("ORIGINAL_A_NUM", servedMSISDN);
                servedMSISDN = servedMSISDN.toString().substring(2);
                commonAttributes.put("SERVED_MSISDN", servedMSISDN);
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN.toString());
                if (ddk != null) {
                    commonAttributes.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    commonAttributes.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                }
            }

            Object listOfTrafficVolumes = data.get("listOfTrafficVolumes");
            Object serviceList = data.get("listOfServiceData");
            List<LinkedHashMap<String, Object>> listOfServiceData = handleListOfServiceData(serviceList);
            LinkedHashMap<String, Object> splitDataset = splitByRatingGroup(listOfServiceData, commonAttributes);
            LinkedHashMap<String, Object> finalDataset = new LinkedHashMap<>();
            for (Map.Entry<String, Object> recEntry : splitDataset.entrySet()) {
                //ignore zero usage sessions
                Object rec = recEntry.getValue();
                if (rec instanceof LinkedHashMap){
                    LinkedHashMap<String, Object> rgData = (LinkedHashMap<String, Object>) rec;
                    long totalVolume = (Long) rgData.get("TOTAL_VOLUME");
                    if (totalVolume > 0) {
                        finalDataset.put(recEntry.getKey(),  rgData);
                        float billableVolume = totalVolume / 1024;
                        rgData.put("BILLABLE_VOLUME", ((Double) Math.ceil(billableVolume)).intValue());
                    }
                }
            }
            return finalDataset;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }


    @Override
    @SuppressWarnings("Duplicates")
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

    private String extractIpAddress(Object addressList) {
        String extract;
        if(addressList instanceof List){
            Set<String> ips = new HashSet<>((ArrayList<String>) addressList);
            extract = StringUtils.join(ips, "|");
        } else {
            extract = String.valueOf(addressList);
        }
        return extract;
    }

    private List<LinkedHashMap<String, Object>> handleListOfServiceData(Object serviceData) {
        List<LinkedHashMap<String, Object>> listOfServiceData = new ArrayList<>();
        if (serviceData instanceof List) {
            for (Object serviceDataArray : (ArrayList) serviceData) {
                if (serviceDataArray instanceof List) {
                    for (Object serviceDataItem : (ArrayList) serviceDataArray) {
                        if (serviceDataItem instanceof LinkedHashMap) {
                            LinkedHashMap<String, Object> serviceEntry = (LinkedHashMap<String, Object>) serviceDataItem;
                            LinkedHashMap<String, Object> serviceEntries = new LinkedHashMap<>();
                            serviceEntries.put("ratingGroup", serviceEntry.get("ratingGroup"));
                            serviceEntries.put("localSequenceNumber", serviceEntry.get("localSequenceNumber"));
                            serviceEntries.put("timeOfFirstUsage", serviceEntry.get("timeOfFirstUsage"));
                            serviceEntries.put("timeOfLastUsage", serviceEntry.get("timeOfLastUsage"));
                            serviceEntries.put("timeUsage", serviceEntry.get("timeUsage"));
                            serviceEntries.put("serviceConditionChange", serviceEntry.get("serviceConditionChange"));
                            serviceEntries.put("qoSInformationNeg", serviceEntry.get("qoSInformationNeg"));
                            serviceEntries.put("sgsnAddress", extractIpAddress(serviceEntry.get("sgsnAddress")));
                            serviceEntries.put("dataVolumeFBCUplink", serviceEntry.get("dataVolumeFBCUplink"));
                            serviceEntries.put("dataVolumeFBCDownlink", serviceEntry.get("dataVolumeFBCDownlink"));
                            serviceEntries.put("timeOfReport", serviceEntry.get("timeOfReport"));

                            listOfServiceData.add(serviceEntries);
                        }
                    }
                }
            }
        }
        return listOfServiceData;
    }


    private LinkedHashMap<String, Object> splitByRatingGroup(List<LinkedHashMap<String, Object>> listOfServiceData,
                                                             LinkedHashMap<String, Object> commonAttributes) {
        LinkedHashMap<String, Object>  rgMappings = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> serviceData : listOfServiceData) {
            try {

                LinkedHashMap<String, Object> rgData = new LinkedHashMap<>(commonAttributes);
                String ratingGroup = String.valueOf(serviceData.get("ratingGroup"));
                String localSequenceNumber = String.valueOf(serviceData.get("localSequenceNumber"));
                String key = ratingGroup + "|" + localSequenceNumber;
                rgData.put("RATING_GROUP", serviceData.get("ratingGroup"));
                rgData.put("LOCAL_SEQ_NUM", serviceData.get("localSequenceNumber"));
                rgData.put("SGSN_ADDRESS", serviceData.get("sgsnAddress"));
                rgData.put("SERVICE_COND_CHANGE", serviceData.get("serviceConditionChange"));
                rgData.put("QOS_INFO", serviceData.get("qoSInformationNeg"));
                Object dataVolumeFBCUplink = serviceData.get("dataVolumeFBCUplink");
                Object dataVolumeFBCDownlink = serviceData.get("dataVolumeFBCDownlink");
                Object timeOfFirstUsage = serviceData.get("timeOfFirstUsage");
                Object timeOfLastUsage = serviceData.get("timeOfLastUsage");
                Object timeOfReport = serviceData.get("timeOfReport");

                long totalVolume = 0L;
                if (dataVolumeFBCUplink != null) {
                    totalVolume += Long.parseLong((String.valueOf(dataVolumeFBCUplink)));
                    rgData.put("DATA_VOLUME_FBC_UPLINK", dataVolumeFBCUplink);
                }
                if (dataVolumeFBCDownlink != null) {
                    totalVolume += Long.parseLong((String.valueOf(dataVolumeFBCDownlink)));
                    rgData.put("DATA_VOLUME_FBC_DOWNLINK", dataVolumeFBCDownlink);
                }

                rgData.put("TOTAL_VOLUME", totalVolume);
                Date timeOfFirstUsageTime = sdfS.get().parse(timeOfFirstUsage.toString());
                Date timeOfLastUsageTime = sdfS.get().parse(timeOfLastUsage.toString());
                Date timeOfReportTime = sdfS.get().parse(timeOfReport.toString());
                rgData.put("TIME_FIRST_USAGE", sdfT.get().format(timeOfFirstUsageTime));
                rgData.put("TIME_LAST_USAGE", sdfT.get().format(timeOfLastUsageTime));
                rgData.put("REPORT_TIME", sdfT.get().format(timeOfReportTime));

                rgMappings.put(key, rgData);
            }  catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return rgMappings;
    }
}

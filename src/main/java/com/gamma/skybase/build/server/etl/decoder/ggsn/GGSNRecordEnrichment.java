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
    Object serviceList = null;

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> commonAttributes = new LinkedHashMap<>();
        try {
            Object eventType = data.get("eventType");
            Object recordType = data.get("recordType");
            Object servedIMSI = data.get("servedIMSI");
            Object pGWAddress = data.get("PGWAddress");

            String strpGWAddress = pGWAddress.toString();
            strpGWAddress = strpGWAddress.replace("[{pGWAddress=","");
            strpGWAddress = strpGWAddress.replace("}]","");

            Object servingNodeAddress = data.get("ServingNodeAddress");

            String strservingNodeAddress = servingNodeAddress.toString();
            strservingNodeAddress = strservingNodeAddress.replace("[{servingNodeAddress=" , "");
            strservingNodeAddress = strservingNodeAddress.replace("}]" , "");

            Object servedPDPPDNAddress = data.get("servedPDPPDNAddress");

            String strservedPDPPDNAddress = servedPDPPDNAddress.toString();
            strservedPDPPDNAddress = strservedPDPPDNAddress.replace("[{IPAddress=[{IPV4BinAddress=" , "");
            strservedPDPPDNAddress = strservedPDPPDNAddress.replace("}]}]" , "");

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
            Object servingNodeType = data.get("ServingNodeType");
            Object fileName = data.get("fileName");
            Object recordExtensions = data.get("recordExtensions");
            Object serviceEventUrl = getServiceEventUrlIfPresent(recordExtensions);

            commonAttributes.put("EVENT_TYPE", eventType);
            commonAttributes.put("RECORD_TYPE", recordType);
            commonAttributes.put("SERVED_IMSI", servedIMSI);
            commonAttributes.put("SERVED_IMEI", servedIMEISV);
            if (servedIMEISV != null && !servedIMEISV.toString().isEmpty())
                commonAttributes.put("tac", servedIMEISV.toString().substring(0, 8));

            commonAttributes.put("PGW_ADDRESS", strpGWAddress);
            commonAttributes.put("SGSN_ADDRESS", strservingNodeAddress);
            commonAttributes.put("PDP_ADDRESS", strservedPDPPDNAddress);
            commonAttributes.put("CHARGING_ID", chargingID);
            commonAttributes.put("APN_NAME", accessPointNameNI);
            commonAttributes.put("PDP_TYPE", pdpPDNType);
            commonAttributes.put("DYNAMIC_ADDRESS_FLAG", dynamicAddressFlag);
            commonAttributes.put("ORIGINAL_DUR", duration);
            commonAttributes.put("SERVICE_EVENT_URL", serviceEventUrl);

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
                commonAttributes.put("SRV_TYPE_KEY", srvTypeKey);
                commonAttributes.put("chargingCharacteristics", chargingCharacteristics);
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
            //if (data.get("listOfServiceData").equals(null) == false && data.get("listOfServiceData").toString().equalsIgnoreCase(null) == false)

            if (data.containsKey("listOfServiceData") == true)
            {
                serviceList = data.get("listOfServiceData");
            }else{
                //nothing to do
            }

            /*if (data.get("listOfServiceData").equals(null) == false)
            {
                serviceList = data.get("listOfServiceData");
            }*/

            //Object serviceList = data.get("listOfServiceData");
            List<LinkedHashMap<String, Object>> listOfServiceData = handleListOfServiceData(serviceList);
            LinkedHashMap<String, Object> splitDataset = splitByRatingGroup(listOfServiceData, commonAttributes);
            LinkedHashMap<String, Object> finalDataset = new LinkedHashMap<>();
            for (Map.Entry<String, Object> recEntry : splitDataset.entrySet()) {  // ignore zero usage sessions
                Object rec = recEntry.getValue();
                if (rec instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> rgData = (LinkedHashMap<String, Object>) rec;
                    long totalVolume = (Long) rgData.get("TOTAL_VOLUME");
                    if (totalVolume > 0) {
                        finalDataset.put(recEntry.getKey(), rgData);
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

    private List<LinkedHashMap<String, Object>> handleListOfServiceData(Object serviceData) {
        if (serviceData instanceof ArrayList) {
            ArrayList<LinkedHashMap<String, Object>> sd = (ArrayList<LinkedHashMap<String,java.lang.Object>>) serviceData;
            LinkedHashMap<String , Object> csc = sd.get(0);
            ArrayList<LinkedHashMap<String , Object>> list = (ArrayList<LinkedHashMap<String, Object>>) csc.get("ChangeOfServiceCondition");

//            ArrayList<LinkedHashMap<String,Object>> x = (ArrayList<LinkedHashMap<String, Object>>) sd.get();
//            List<LinkedHashMap<String, Object>> x = (List<LinkedHashMap<String,java.lang.Object>>) sd.get("ChangeOfServiceCondition");
//            ArrayList<LinkedHashMap<String, Object> > list = (ArrayList<LinkedHashMap<String, Object>>) sd.get("ChangeOfServiceCondition");
            if (list.size()>0) {
                LinkedHashMap<String, Object> serviceEntry = (LinkedHashMap<String, Object>) list.get(0);
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
                list.add(serviceEntries);
            }
            return list;
        }
        return null;
    }

    private LinkedHashMap<String, Object> splitByRatingGroup(List<LinkedHashMap<String, Object>> listOfServiceData,
                                                             LinkedHashMap<String, Object> commonAttributes) {
        LinkedHashMap<String, Object> rgMappings = new LinkedHashMap<>();
        if (listOfServiceData != null){
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

                    Object datavolumeFBCUplink = serviceData.get("datavolumeFBCUplink");
                    Object datavolumeFBCDownlink = serviceData.get("datavolumeFBCDownlink");

                    rgData.put("DATA_VOLUME_FB_UPLINK",serviceData.get("datavolumeFBCUplink"));
                    rgData.put("DATA_VOLUME_FB_DOWNLINK",serviceData.get("datavolumeFBCDownlink"));

                    Object timeOfFirstUsage = serviceData.get("timeOfFirstUsage");
                    Object timeOfLastUsage = serviceData.get("timeOfLastUsage");
                    Object timeOfReport = serviceData.get("timeOfReport");

                    long totalVolume = 0L;
                    if (datavolumeFBCUplink != null) {
                        totalVolume += Long.parseLong((String.valueOf(datavolumeFBCUplink)));
                        rgData.put("DATA_VOLUME_FBC_UPLINK", datavolumeFBCUplink);
                    }
                    if (datavolumeFBCDownlink != null) {
                        totalVolume += Long.parseLong((String.valueOf(datavolumeFBCDownlink)));
                        rgData.put("DATA_VOLUME_FBC_DOWNLINK", datavolumeFBCDownlink);
                    }

                    rgData.put("TOTAL_VOLUME", totalVolume);
                    Date timeOfFirstUsageTime = sdfS.get().parse(timeOfFirstUsage.toString());
                    Date timeOfLastUsageTime = sdfS.get().parse(timeOfLastUsage.toString());
                    Date timeOfReportTime = sdfS.get().parse(timeOfReport.toString());
                    rgData.put("TIME_FIRST_USAGE", sdfT.get().format(timeOfFirstUsageTime));
                    rgData.put("TIME_LAST_USAGE", sdfT.get().format(timeOfLastUsageTime));
                    rgData.put("REPORT_TIME", sdfT.get().format(timeOfReportTime));

                    rgMappings.put(key, rgData);
                } catch (ParseException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return rgMappings;
    }

    private Object getServiceEventUrlIfPresent(Object recordExtensions) {
        if (recordExtensions == null) return "";

        List<Object> recordExtensionList = (List<Object>) recordExtensions;
        LinkedHashMap<String, Object> recordExtensionEntry = (LinkedHashMap<String, Object>) recordExtensionList.stream().findFirst().get();
        Object serviceList = recordExtensionEntry.get("serviceList");
        if (serviceList == null) return "";

        String serviceEventUrl = "";
        ArrayList<Object> servList = (ArrayList<Object>) serviceList;
        if (servList.size() > 0) {
            Object serviceObjList = servList.stream().findFirst().get();
            List<Object> serviceEntryList = (List<Object>) serviceObjList;
            if (serviceEntryList.size() > 0) {
                Object serviceObj = serviceEntryList.stream().findFirst().get();
                if (serviceObj instanceof LinkedHashMap) {
                    LinkedHashMap<String, Object> service = (LinkedHashMap<String, Object>) serviceObj;
                    if (service.containsKey("url"))
                        serviceEventUrl = (String) service.get("url");
                }
            }
        }
        return serviceEventUrl;
    }
}

package com.gamma.skybase.build.server.etl.tx.postmed;

import com.gamma.components.commons.StringUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by abhi on 12/01/22
 */
@SuppressWarnings("Duplicates, unchecked")
public class PostMedChargingRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(PostMedChargingRecordEnrichment.class);
    private Map<String, Object> additionalRecordParams;
    private final ThreadLocal<SimpleDateFormat> sourceFormatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> targetFormatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(data);
        //removing an unmapped column
        txRecord.remove("EXTRA1");
        String sourceName = (String) additionalRecordParams.get("source");
        try {
            String teleServiceCode = (String) data.get("TELESERVICECODE");
            String sessionId = (String) data.get("SESSIONID");
            String extText = (String) data.get("EXTTEXT");
            String eventStartTime = null;
            String streamName, charge, servedMsisdn;
            String eventTypeKey = null, eventDirectionKey = null, srvTypeKey = null, nwIndKey = null;
            Object triggerTime = data.get("TRIGGERTIME");
            boolean isPost = sourceName.contains("post");
            if (triggerTime != null) eventStartTime = (String) triggerTime;

            if (StringUtility.arrayContainsIgnoreCase(new String[]{"eric_pm_gy_pre", "eric_pm_gy_post"},  sourceName)) {
                //event_start_time fix
                Object chargetime = data.get("CHARGETIME");
                if (chargetime != null) eventStartTime = (String) chargetime;

                streamName = "gy";
                servedMsisdn = (String) data.get("SUBSCRIBERID");
                Object finalCharge = data.get("FINALCHARGE");
                eventTypeKey = "4";
                eventDirectionKey = "1";
                charge = (String) finalCharge;
                if (isPost) {
                    //todo out roamer record needed to be segregated
                    srvTypeKey = "1";
                } else {
                    // zain does not allow prepaid subscribers to have data roaming
                    srvTypeKey = "2";
                }
                txRecord.put("RATING_GROUP", teleServiceCode);
            } else if (StringUtility.arrayContainsIgnoreCase(new String[]{"eric_pm_scf_pre", "eric_pm_scf_post"},  sourceName)) {
                //event_start_time fix
                Object timezoneforstartofcharging = data.get("TIMEZONEFORSTARTOFCHARGING");
                if (timezoneforstartofcharging != null) eventStartTime = (String) timezoneforstartofcharging;

                streamName = "scf";
                servedMsisdn = (String) data.get("ACCOUNTNUMBER");
                Object finalChargeOfCCall = data.get("FINALCHARGEOFCALL");
                charge = (String) finalChargeOfCCall;
                String originLocationInfo = (String) data.get("ORIGLOCATIONINFO");
                String trafficCaseId = (String) data.get("TRAFFICCASEID");
                eventDirectionKey = "1";
                if (originLocationInfo.startsWith(countryCode)) {
                    srvTypeKey = isPost ? "1" : "2";
                } else {
                    srvTypeKey = isPost ? "5" : "6";
                }

                if (Integer.parseInt(trafficCaseId) >= 7) {
                    srvTypeKey = isPost ? "5" : "6";
                    //this is Originated Roaming calls and SMSs
                    if ("7".equalsIgnoreCase(trafficCaseId)) {
                        eventDirectionKey = "1";
                    }
                    //this is call forward Roaming calls and SMSs
                    else if ("8".equalsIgnoreCase(trafficCaseId)) {
                        eventDirectionKey = "2";
                    }
                    //this is terminated Roaming calls and SMSs
                    else if ("9".equalsIgnoreCase(trafficCaseId)) {
                        eventDirectionKey = "2";
                    }
                }
            }  else {
                streamName = "scapv2";
                servedMsisdn = (String) data.get("SUBSCRIBERID");
                Object finalCharge = data.get("FINALCHARGE");
                charge = (String) finalCharge;
                eventDirectionKey = "1";
                srvTypeKey = isPost ? "1" : "2";
                if (StringUtility.arrayContainsIgnoreCase(new String[]{"0", "4", "5"}, teleServiceCode)) {
                    //roaming use case
                    srvTypeKey = isPost ? "5" : "6";
                }
            }

            if (servedMsisdn != null) {
                String formattedServedMsisdn;
                if (StringUtils.isNumeric(servedMsisdn)) {
                    formattedServedMsisdn = StringUtils.stripStart(servedMsisdn, "0");
                    formattedServedMsisdn = countryCode + formattedServedMsisdn;
                } else {
                    formattedServedMsisdn = servedMsisdn;
                }
                txRecord.put("SERVED_MSISDN", formattedServedMsisdn);
            }

            if (eventTypeKey == null) eventTypeKey = getEventTypeKey(teleServiceCode, extText, sessionId);
            txRecord.put("EVENT_TYPE_KEY", eventTypeKey);

            if ("scf".equalsIgnoreCase(streamName)) {
                String networkId = (String) data.get("NETWORKID");
                String calledPartyNet = (String) data.get("CALLEDPARTYNET");
                String calledNumber = (String) data.get("CALLEDNUMBER");
                // this is onnet calls and SMSs ,b numbers belong to zain
                if ("402".equalsIgnoreCase(networkId)) {
                    nwIndKey = "1";
                }

                // this is onnet IVRs calls and shortcode SMSs ,b numbers belong to zain
                else if ("0".equalsIgnoreCase(networkId) && "2".equalsIgnoreCase(calledPartyNet)) {
                    nwIndKey = "1";
                }

                // this is onnet Voice Mail
                else if ("0".equalsIgnoreCase(networkId) && calledNumber.contains("B")) {
                    nwIndKey = "1";
                }

                // this is offnet calls and SMSs ,b numbers belong to SUDANI && MTN operator
                else if (StringUtility.arrayContainsIgnoreCase(new String[] {"401", "403"}, networkId)) {
                    nwIndKey = "2";
                }

                // this is offnet calls and SMSs ,b numbers belong  line telephone
                else if ("0".equalsIgnoreCase(networkId) && !StringUtility.arrayContainsIgnoreCase(new String[]{"2", "4"}, calledPartyNet)
                    && calledNumber.startsWith("015") && calledNumber.startsWith("017") &&   calledNumber.startsWith("018")) {
                    nwIndKey = "2";
                }

                //this is likely to be from sudatel
                else if ("0".equalsIgnoreCase(networkId) && !StringUtility.arrayContainsIgnoreCase(new String[]{"2", "4"}, calledPartyNet)
                    && !calledNumber.startsWith("015") && calledNumber.startsWith("017") && calledNumber.startsWith("018") && !calledNumber.contains("B")) {
                    nwIndKey = "2";
                }

                // this is international calls and SMSs ,b numbers belong to international
                else if ("4".equalsIgnoreCase(calledPartyNet)) {
                    nwIndKey = "3";
                }
                txRecord.put("NW_IND_KEY", nwIndKey);
            }

            txRecord.put("CHARGE", charge);
            txRecord.put("EVENT_DIRECTION_KEY", eventDirectionKey);
            txRecord.put("SRV_TYPE_KEY", srvTypeKey);

            if (eventStartTime != null) {
                txRecord.put("XDR_DATE", targetFormatter.get().format(sourceFormatter.get().parse(eventStartTime)));
            }
            txRecord.put("POPULATION_DATE", targetFormatter.get().format(new Date()));
            txRecord.put("FILE_NAME", additionalRecordParams.get("fileName"));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return txRecord;
    }

    private String getEventTypeKey(String teleServiceCode, String extText, String session) {
        String eventTypeKey = "-99";
        switch (teleServiceCode.trim()) {
            case "0":
                eventTypeKey = "1";
                break;
            case "4":
            case "5":
                eventTypeKey = "2";
                break;
            case "65530":
                //huawei msdp
                if (session.toLowerCase().contains("huawei")) eventTypeKey = "21";
                //esb
                if (extText.toLowerCase().contains("esb")) eventTypeKey = "22";
                break;
            case "65533":
                //jinny psms
                eventTypeKey = "23";
                break;
            case "65535":
                //MMS
                eventTypeKey = "3";
                break;
            case "65534":
                //RBT
                eventTypeKey = "15";
                break;
            case "65577":
                //DSP
                eventTypeKey = "18";
                break;
        }
        return eventTypeKey;
    }

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        this.additionalRecordParams = (Map<String, Object>) request.getOptionalParams();
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if(data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        }
        else response.setResponseCode(false);
        return response;
    }
}

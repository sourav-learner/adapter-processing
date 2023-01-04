package com.gamma.skybase.build.server.etl.tx.msc;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.utility.MTrunkGroup;
import com.gamma.skybase.build.utility.TrunkGroupConfigManager;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;

/**
 * Created by abhi on 2019-03-05
 */
@SuppressWarnings("Duplicates")
public class GMSCRecordEnrichment implements IEnrichment {

    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private final Logger logger = LoggerFactory.getLogger(GMSCRecordEnrichment.class);

    private static final String FORMAT1 = "yyMMddHHmmss";
    private static final String FORMAT2 = "yyyyMMdd HH:mm:ss";

    private static final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT1));
    private static final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT2));

    private final Date currentTime = new Date();
    private final String opcoName = AppConfig.instance().getProperty("app.datasource.opconame");
    private final String isoCode = AppConfig.instance().getProperty("app.datasource.isocode");
    private final String operatorCode = AppConfig.instance().getProperty("app.datasource.opcode");
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

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
        try {
            String dupInd = "9";
            String callType = record.get("eventType").toString();
            record.put("callType", callType);
            Object recordSequenceNumber = record.get("recordSequenceNumber");
            Object dateForStartOfCharge = record.get("dateForStartOfCharge");
            Object timeForStartOfCharge = record.get("timeForStartOfCharge");
            Object timeForStopOfCharge = record.get("timeForStopOfCharge");
            Object callIdentificationNumber = record.get("callIdentificationNumber");
            Object callingPartyNumber = record.get("callingPartyNumber");
            Object originatingAddress = record.get("originatingAddress");
            Object calledPartyNumber = record.get("calledPartyNumber");
            Object destinationAddress = record.get("destinationAddress");
            Object translatedNumber = record.get("translatedNumber");
            Object mobileStationRoamingNumber = record.get("mobileStationRoamingNumber");
            Object partialOutputRecNum = record.get("partialOutputRecNum");
            Object calledSubscriberIMEI = record.get("calledSubscriberIMEI");
            Object callingSubscriberIMEI = record.get("callingSubscriberIMEI");
            Object serviceCentreAddress = record.get("serviceCentreAddress");
            Object networkCallReference = record.get("networkCallReference");
            Object exchangeIdentity = record.get("exchangeIdentity");
            Object redirectingNumber = record.get("redirectingNumber");
            Object callingSubscriberIMSI = record.get("callingSubscriberIMSI");
            Object calledSubscriberIMSI = record.get("calledSubscriberIMSI");
            Object redirectingIMSI = record.get("redirectingIMSI");
            Object firstCallingLocInfo = record.get("firstCallingLocationInformation");
            Object firstCalledLocInfo = record.get("firstCalledLocationInformation");

//            If CALLTYPE=’MSTerminatingSMSinMSC’ or ‘MSTerminating’ or ‘RoamingCallForwarding’ map normalized TRANSLATEDNUMBER
//            to SERVED_MSISDN. If TRANSLATEDNUMBER is null map CALLEDPARTYNUMBER.
//
//            Else if CALLTYPE=’CallForwarding’ map normalized  REDIRECTINGNUMBER to SERVED_MSISDN. Else map normalized CALLINGPARTYNUMBER to SERVED_MSISDN
//            Follow the NORMALIZATION RULE for SERVED_MSISDN.
//
            String otherMSISDN = "", servedMSISDN = "", thirdPartyMSISDN = "", lastCellId = "",
                    servedIMSI = null, servedIMEI = "", originalANo = "", originalBNo = "", cgiIdKey = "", zeroChgInd = "";
            switch (callType) {
                case "eventModule":
                case "sSProcedure":
                case "iSDNSSProcedure":
                case "iNIncomingCall":
                case "iNOutgoingCall":
                case "transitINOutgoingCall":
                case "sCFChargingOutput":
                case "locationServices":
                    return null;

                case "mSTerminatingSMSinMSC":
                case "mSTerminatingSMSinSMSGMSC":
                    if (originatingAddress != null) originalANo = originatingAddress.toString();
                    if (calledPartyNumber != null) originalBNo = calledPartyNumber.toString();

                    servedMSISDN = originalBNo;
                    if (calledSubscriberIMEI != null) servedIMEI = calledSubscriberIMEI.toString();
                    if (calledSubscriberIMSI != null) servedIMSI = calledSubscriberIMSI.toString();

                    if (originatingAddress != null) otherMSISDN = originatingAddress.toString();

                    if (firstCalledLocInfo != null) {
                        cgiIdKey = firstCalledLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }
                    zeroChgInd = "9";
                    break;

                case "mSTerminating":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();
                    if (translatedNumber != null) originalBNo = translatedNumber.toString();
                    else if (calledPartyNumber != null) originalBNo = calledPartyNumber.toString();

                    servedMSISDN = originalBNo;
                    if (calledSubscriberIMEI != null) servedIMEI = calledSubscriberIMEI.toString();
                    if (calledSubscriberIMSI != null) servedIMSI = calledSubscriberIMSI.toString();

                    if (translatedNumber != null) otherMSISDN = translatedNumber.toString();
                    else if (callingPartyNumber != null) otherMSISDN = callingPartyNumber.toString();

                    if (firstCalledLocInfo != null) {
                        cgiIdKey = firstCalledLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }

                    break;

                case "roamingCallForwarding":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();

                    if (translatedNumber != null) originalBNo = translatedNumber.toString();
                    else if (calledPartyNumber != null) originalBNo = calledPartyNumber.toString();

                    servedMSISDN = originalBNo;
                    if (calledSubscriberIMEI != null) servedIMEI = calledSubscriberIMEI.toString();
                    if (calledSubscriberIMSI != null) servedIMSI = calledSubscriberIMSI.toString();

                    if (mobileStationRoamingNumber != null) otherMSISDN = mobileStationRoamingNumber.toString();

                    if (callingPartyNumber != null) thirdPartyMSISDN = callingPartyNumber.toString(); // 18

                    if (firstCallingLocInfo != null) {
                        cgiIdKey = firstCallingLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }
                    break;


                case "callForwarding":
                case "iSDNCallForwarding":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();
                    if (redirectingNumber != null) originalBNo = redirectingNumber.toString();

                    if (translatedNumber != null) record.put("ORIGINAL_C_NUM", translatedNumber.toString());
                    else if (calledPartyNumber != null) record.put("ORIGINAL_C_NUM", calledPartyNumber.toString());

                    if (translatedNumber != null) otherMSISDN = translatedNumber.toString();
                    else if (calledPartyNumber != null) otherMSISDN = calledPartyNumber.toString();

                    if (redirectingNumber != null) servedMSISDN = redirectingNumber.toString();
                    if (callingSubscriberIMEI != null) servedIMEI = callingSubscriberIMEI.toString();
                    if (redirectingIMSI != null) servedIMSI = redirectingIMSI.toString();

                    if (translatedNumber != null) thirdPartyMSISDN = translatedNumber.toString();
                    else if (callingPartyNumber != null) thirdPartyMSISDN = callingPartyNumber.toString(); // 18

                    if (firstCallingLocInfo != null) {
                        cgiIdKey = firstCallingLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }

                    break;

                case "mSOriginatingSMSinMSC":
                case "mSOriginatingSMSinSMSIWMSC":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();

                    servedMSISDN = originalANo;
                    if (callingSubscriberIMEI != null) servedIMEI = callingSubscriberIMEI.toString();
                    if (callingSubscriberIMSI != null) servedIMSI = callingSubscriberIMSI.toString();

                    if (destinationAddress != null) originalBNo = destinationAddress.toString();
                    if (destinationAddress != null) otherMSISDN = destinationAddress.toString();
                    if (calledPartyNumber != null) otherMSISDN = calledPartyNumber.toString();

                    if (firstCallingLocInfo != null) {
                        cgiIdKey = firstCallingLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }
                    zeroChgInd = "9";
                    break;

                case "mSOriginating":
                case "iSDNOriginating":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();
                    if (translatedNumber != null) originalBNo = translatedNumber.toString();
                    else if (calledPartyNumber != null) originalBNo = calledPartyNumber.toString();

                    if (translatedNumber != null) otherMSISDN = translatedNumber.toString();
                    else if (calledPartyNumber != null) otherMSISDN = calledPartyNumber.toString();

                    servedMSISDN = originalANo;
                    if (callingSubscriberIMEI != null) servedIMEI = callingSubscriberIMEI.toString();
                    if (callingSubscriberIMSI != null) servedIMSI = callingSubscriberIMSI.toString();

                    if (firstCallingLocInfo != null) {
                        cgiIdKey = firstCallingLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }

                    break;

                case "transit":
                    if (callingPartyNumber != null) originalANo = callingPartyNumber.toString();
                    if (translatedNumber != null) originalBNo = translatedNumber.toString();
                    else if (calledPartyNumber != null) originalBNo = calledPartyNumber.toString();

                    if (callingSubscriberIMEI != null) servedIMEI = callingSubscriberIMEI.toString();
                    if (callingSubscriberIMSI != null) servedIMSI = callingSubscriberIMSI.toString();
                    servedMSISDN = originalANo;

                    if (translatedNumber != null) otherMSISDN = translatedNumber.toString();
                    else if (calledPartyNumber != null) otherMSISDN = calledPartyNumber.toString();

                    if (firstCallingLocInfo != null) {
                        cgiIdKey = firstCallingLocInfo.toString();
                        if (cgiIdKey.length() > 5) lastCellId = cgiIdKey.substring(cgiIdKey.length() - 5);
                    }
                    break;

                default:
                    logger.info("Unknown Event Type !!!");
                    break;
            }

            if (originalANo.length() > 14) {
                originalANo = transformationLib.specialCaller(originalANo);
            }
            record.put("ORIGINAL_A_NUM", originalANo);

            if (originalBNo.length() > 14) {
                originalBNo = transformationLib.specialCaller(originalBNo);
            }
            record.put("ORIGINAL_B_NUM", originalBNo);
            record.put("CGI_ID_KEY", ("".equals(cgiIdKey) ? "-99" : cgiIdKey));

            if (StringUtils.isNotEmpty(lastCellId) && !"-99".equalsIgnoreCase(lastCellId)) {
                lastCellId = StringUtils.leftPad(String.valueOf(Math.abs(Integer.parseInt(lastCellId))), 5, "0");
            }
            record.put("LAST_CELL_ID", ("".equals(lastCellId) ? "-99" : lastCellId));

            if (recordSequenceNumber != null) record.put("EDR_SEQ_NUM", recordSequenceNumber.toString());// 1
            record.put("POPULATION_DATE_TIME", sdfT.get().format(currentTime));
            record.put("SYS_ID_KEY", "16002");

            if (networkCallReference != null) record.put("CALL_REFERENCE_NUMBER", networkCallReference);

            if (exchangeIdentity != null) record.put("NODE_ADDRESS", exchangeIdentity);


            Object faultCode = record.get("faultCode");
            if (faultCode != null) record.put("TERMINATION_REASON_KEY", faultCode);

            String startDate = "";
            if (dateForStartOfCharge != null) startDate = (dateForStartOfCharge.toString());
            String startTime = "";
            if (timeForStartOfCharge != null) startTime = (timeForStartOfCharge.toString());
            String endTime = "";
            if (timeForStopOfCharge != null) endTime = (timeForStopOfCharge.toString());

            String startDateString = startDate + startTime;
            Date eventStartTime = null;
            try {
                eventStartTime = sdfS.get().parse(startDateString);
                if (eventStartTime != null) {
                    record.put("EVENT_START_TIME", sdfT.get().format(eventStartTime)); // 2
                    record.put("XDR_DATE", sdfT.get().format(eventStartTime)); // 2
                    record.put("GENERATED_FULL_DATE", new SimpleDateFormat("yyyyMMdd").format(eventStartTime) + " 00:00:00"); //31
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            Date eventEndTime = transformationLib.getEventEndTime(startDate, startTime, endTime);
            if (eventEndTime != null) record.put("EVENT_END_TIME", sdfT.get().format(eventEndTime));

            long duration = 0;
            Object chargeableDuration = record.get("chargeableDuration");
            if (chargeableDuration != null) {
                String cd = chargeableDuration.toString();
                LocalTime time = LocalTime.parse(cd, DateTimeFormatter.ofPattern("HHmmss"));
                duration = time.getHour() * 60 * 60 + time.getMinute() * 60 + time.getSecond();
                record.put("ORIGINAL_DUR", duration);
            }
            long bp = (long) Math.ceil((double) duration / 60);
            record.put("BILLABLE_PULSE", bp);

            if (zeroChgInd.equals(""))
                if (bp == 0) zeroChgInd = "0";
                else zeroChgInd = "9";
            record.put("ZERO_CHRG_IND", zeroChgInd);
            String zeroDurationInd = "0";
            if (duration == 0) {
                zeroDurationInd = "1";
            }
            record.put("ZERO_DURATION_IND", zeroDurationInd);

            Object bearerServiceCode = record.get("bearerServiceCode");
            Object teleServiceCode = record.get("teleServiceCode");
            String basicServiceKey;
            if (bearerServiceCode != null) basicServiceKey = bearerServiceCode.toString();
            else if (teleServiceCode != null) basicServiceKey = teleServiceCode.toString();
            else basicServiceKey = "-99";
            record.put("BASIC_SERVICE_KEY", basicServiceKey);

            if (!otherMSISDN.equals("")) {
                otherMSISDN = transformationLib.normalizeMSC(otherMSISDN, countryCode);
                record.put("OTHER_MSISDN", otherMSISDN);
            }
            if (!servedMSISDN.equals("")) {
                servedMSISDN = transformationLib.normalizeMSC(servedMSISDN, countryCode);
                record.put("SERVED_MSISDN", servedMSISDN);
            }

            if (!thirdPartyMSISDN.equals("")) {
                thirdPartyMSISDN = transformationLib.normalizeMSC(thirdPartyMSISDN, countryCode);
                record.put("THIRD_PARTY_MSISDN", thirdPartyMSISDN);
            }

            String servedMSRN = "";
            if (mobileStationRoamingNumber != null) {
                servedMSRN = transformationLib.normalizeMSC(mobileStationRoamingNumber.toString(), countryCode);
                record.put("SERVED_MSRN", servedMSRN);
            }

            if (servedIMSI != null) record.put("SERVED_IMSI", servedIMSI);
            record.put("SERVED_IMEI", servedIMEI);

            if (mobileStationRoamingNumber != null)
                record.put("ORIGINAL_MSRN", mobileStationRoamingNumber);

            if (partialOutputRecNum != null)
                record.put("PARTIAL_RECORD_NUM", partialOutputRecNum.toString());

            if (recordSequenceNumber != null)
                record.put("EDR_SEQ_NUM", recordSequenceNumber.toString());

            if (serviceCentreAddress != null)
                record.put("SERVICE_CENTRE_ADDRESS", serviceCentreAddress.toString());

            String eventTypeKey = transformationLib.getMSCEventTypeKey(otherMSISDN, callType);
            if (eventTypeKey != null) record.put("EVENT_TYPE_KEY", eventTypeKey);


            String eventDirectionKey = transformationLib.getMSCEventDirectionKey(otherMSISDN, callType);
            if (eventDirectionKey != null) record.put("EVENT_DIRECTION_KEY", eventDirectionKey);

            String chgUnitIdKey = transformationLib.getMSCChgUnitIdKey(otherMSISDN, callType);
            if (chgUnitIdKey != null) record.put("CHRG_UNIT_ID_KEY", chgUnitIdKey);

            if (otherMSISDN != null) {
                String nwIndKey = null;
                if (servedIMSI != null && !servedIMSI.startsWith(operatorCode)) nwIndKey = "3";
                if (NumberUtils.isCreatable(otherMSISDN)) {
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
                        if (nwIndKey == null) {
                            if ("DOMESTIC".equalsIgnoreCase(ddk.getDialDigitDesc())) {
                                if ("125".equalsIgnoreCase(ddk.getNopIdKey())) nwIndKey = "1";
                                else nwIndKey = "2";
                            } else {
                                nwIndKey = "3";
                            }
                        }
                        record.put("NW_IND_KEY", nwIndKey);
                    }
                } else {
                    record.put("OTHER_MSISDN_ISO_CODE", isoCode);
                    record.put("OTHER_MSISDN_DEST_TYPE", "DOMESTIC");
                    record.put("OTHER_OPER", opcoName);
                    nwIndKey = nwIndKey == null ? "1" : nwIndKey;
                    record.put("NW_IND_KEY", nwIndKey);
                }
            }

            String fn = record.get("fileName").toString();
            String neKey = fn.substring(0, fn.indexOf('_'));

            String neIDKey = transformationLib.getNEIdKey(neKey);
            record.put("NE_ID_KEY", neIDKey);

            String recordTypeKey;
            if (partialOutputRecNum == null) recordTypeKey = "4";
            else {
                if (partialOutputRecNum.toString().equalsIgnoreCase("1")) recordTypeKey = "5";
                else {
                    Object lastPartialOutput = record.get("lastPartialOutput");
                    if (lastPartialOutput != null) recordTypeKey = "7";
                    else recordTypeKey = "6";
                }
            }

            record.put("REC_TYPE_ID_KEY", recordTypeKey);
            try {
                if (!(startDateString == null || "".equalsIgnoreCase(startDateString.trim()))) {
                    Date sd = sdfS.get().parse(startDateString);
                    record.put("EVENT_START_TIME_SLOT_KEY", new SimpleDateFormat("yyyyMMdd HH").format(sd) + ":00:00");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

            if (callIdentificationNumber != null) record.put("EVENT_REF_NUM", callIdentificationNumber);

            String iTrunk = "", oTrunk = "";
            Object incomingRoute = record.get("incomingRoute");
            if (incomingRoute != null) {
                iTrunk = incomingRoute.toString();
                record.put("IN_TG_ID_KEY", iTrunk);
            }

            Object outgoingRoute = record.get("outgoingRoute");
            if (outgoingRoute != null) {
                oTrunk = outgoingRoute.toString();
                record.put("OUT_TG_ID_KEY", oTrunk);
            }

            Object sSCode = record.get("sSCode");
            if (sSCode != null) record.put("SUPPLIMENT_CODE_KEY", sSCode);


            if(record.get("sPIdentity") != null){
                record.put("sPIdentity", record.get("sPIdentity").toString());
            }

            if (servedMSISDN.length() > 15) record.put("FLEXI_IND_1 ", "1");
            else record.put("FLEXI_IND_1 ", "0");

            if (servedMSISDN != null && !servedMSISDN.equals("")) {
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSISDN);
                if (ddk != null) {
                    record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("SERVED_MSISDN_NOP_ID_KEY", ddk.getNopIdKey());
                }
            }

            if (servedMSRN != null && !servedMSRN.equals("")) {
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSRN);
                if (ddk != null) {
                    record.put("SERVED_MSRN_DIAL_DIGIT_KEY", ddk.getDialDigitKey());
                    record.put("SERVED_MSRN_NOP_ID_KEY", ddk.getNopIdKey());
                }
            }

            if (thirdPartyMSISDN != null && !thirdPartyMSISDN.equals("")) {
                ReferenceDimDialDigit ddk = transformationLib.getDialedDigitSettings(servedMSRN);
                if (ddk != null) record.put("THIRD_PARTY_NOP_ID_KEY", ddk.getNopIdKey());
            }

            Object subscriptionType = (record.get("subscriptionType") == null) ? "" : record.get("subscriptionType");
            String srvTypeKey = transformationLib.getMSCSrvTypeKey(eventDirectionKey, callType, subscriptionType.toString(), servedMSISDN,
                    servedIMSI, servedMSRN, countryCode, operatorCode);
            if (srvTypeKey == null) srvTypeKey = "-99";
            record.put("SRV_TYPE_KEY", srvTypeKey);

            String flexCol1 = transformationLib.getFlexCol1(srvTypeKey, servedIMSI);
            if (flexCol1 != null) record.put("FLEXI_COL_1", flexCol1);


            String lrn = "";
            Object calledPartyMNPInfo = record.get("calledPartyMNPInfo");
            if (calledPartyMNPInfo != null && calledPartyMNPInfo.toString().length() > 4)
                lrn = calledPartyMNPInfo.toString().substring(2, 5);
            record.put("LRN", lrn);
            if (!servedIMEI.isEmpty()) record.put("tac", servedIMEI.substring(0, 8));

            if (calledPartyMNPInfo != null && calledPartyMNPInfo.toString().length() > 5)
                record.put("CALLED_PARTY_MNP_MSISDN", calledPartyMNPInfo.toString().substring(5));

            String est = "", eet = "";
            try {
                est = sdfT.get().format(eventStartTime);
                eet = sdfT.get().format(eventEndTime);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            if(incomingRoute == null || outgoingRoute == null || recordSequenceNumber == null){
                if(recordSequenceNumber == null) recordSequenceNumber = "";
                if(incomingRoute == null) incomingRoute = "";
                if(outgoingRoute == null) outgoingRoute = "";
            }
            String edrIdKey = recordTypeKey
                    + '|'+ recordSequenceNumber.toString()
                    + '|'+ incomingRoute.toString()
                    + '|'+ outgoingRoute.toString()
                    +'|' + eventTypeKey + '|' + eventDirectionKey + '|' + servedMSISDN
                    + '|' + otherMSISDN + '|' + est + '|' + eet + '|' + duration;
            record.put("EDR_ID_KEY", edrIdKey);

            /* revenue & cost CDRs identification */
            String tag = "";
            MTrunkGroup ito = TrunkGroupConfigManager.instance().getTrunkGroupMap().get(iTrunk);
            MTrunkGroup oto = TrunkGroupConfigManager.instance().getTrunkGroupMap().get(oTrunk);

            if (!"1".equalsIgnoreCase(recordTypeKey)
                    && !"1".equalsIgnoreCase(zeroDurationInd)
                    && !"1".equalsIgnoreCase(dupInd)
                    && !Arrays.asList("0GRI3","TRACO","NOTIC","FWDO","VMAIL","PREP","IVR1O","KATYA","PYROO","0GRI60","0GRI50", "TCIAL2").contains(oTrunk)
                    && !oTrunk.startsWith("TCIA")
                    && !Arrays.asList("CHIVRI","VBPIVRI").contains(iTrunk)
                    && ito != null
                    && !Arrays.asList("S", "Z").contains(ito.getType())){
                tag = "revenue";
            }

            if (!"1".equalsIgnoreCase(recordTypeKey)
                    && !"1".equalsIgnoreCase(zeroDurationInd)
                    && !"1".equalsIgnoreCase(dupInd)
                    && !Arrays.asList("0GRI3","TRACO","NOTIC","FWDO","VMAIL","PREP","IVR1O","KATYA","PYROO").contains(iTrunk)
                    && oto != null
                    && !Arrays.asList("S", "Z").contains(oto.getType())
                    && "O".equalsIgnoreCase(oto.getDirection())){
                tag = "cost";
            }
            record.put("TAGS", tag);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return record;
    }
}

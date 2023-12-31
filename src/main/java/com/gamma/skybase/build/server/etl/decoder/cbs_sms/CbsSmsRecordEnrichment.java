package com.gamma.skybase.build.server.etl.decoder.cbs_sms;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimCbsOfferPayType;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CbsSmsRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsSmsEnrichmentUtil tx = CbsSmsEnrichmentUtil.of(record);

//        STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

//        EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("CUST_LOCAL_STARTDATE", s);
            record.put("XDR_DATE", s);
        });

//        EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("CUST_LOCAL_ENDDATE", s));

//        OBJTYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJTYPE", s));

//        RESULTCODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

//        EVENT_DIRECTION_KEY
        Optional<String> eventDirectionKey = tx.getEventDirectionKey();
        eventDirectionKey.ifPresent(s -> record.put("EVENT_DIRECTION_KEY", s));

//        CHARGING_TIME
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("CHARGING_TIME", s));

//        SEND_RESULT
        Optional<String> sendResult = tx.getSendResult();
        sendResult.ifPresent(s -> record.put("SEND_RESULT", s));

//        REFUND_INDICATOR
        Optional<String> refundIndicator = tx.getRefundIndicator();
        refundIndicator.ifPresent(s -> record.put("REFUND_INDICATOR", s));

//      ON_NET_INDICATOR
        Optional<String> OnNetIndicator = tx.getOnNetIndicator();
        OnNetIndicator.ifPresent(s -> record.put("ON_NET_INDICATOR", s));

//        ONLINE_CHARGING_FLAG
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("ONLINE_CHARGING_FLAG", s));

//        DIALED_NUMBER
        Optional<String> dialedNumber = tx.getDialedNumber();
        dialedNumber.ifPresent(s -> record.put("DIALED_NUMBER", s));

//        ACTUAL_USAGE_PAYG
        Optional<Double> actualUsagePayg = tx.getActualUsagePayg();
        actualUsagePayg.ifPresent(s -> record.put("ACTUAL_USAGE_PAYG", s));

        //        ACTUAL_USAGE_BONUS
        Optional<Double> actualUsageBonus = tx.getActualUsageBonus();
        actualUsageBonus.ifPresent(s -> record.put("ACTUAL_USAGE_BONUS", s));

//        ACTUAL_USAGE_ALLOWANCE
        Optional<Double> actualUsageAllowance = tx.getActualUsageAllowance();
        actualUsageAllowance.ifPresent(s -> record.put("ACTUAL_USAGE_ALLOWANCE", s));

//        RATE_USAGE_PAYG
        Optional<Double> rateUsagePayg = tx.getRateUsagePayg();
        rateUsagePayg.ifPresent(s -> record.put("RATE_USAGE_PAYG", s));

//        RATE_USAGE_BONUS
        Optional<Double> rateUsageBonus = tx.getRateUsageBonus();
        rateUsageBonus.ifPresent(s -> record.put("RATE_USAGE_BONUS", s));

//        RATE_USAGE_ALLOWANCE
        Optional<String> rateUsageAllowance = tx.getRateUsageAllowance();
        rateUsageAllowance.ifPresent(s -> record.put("RATE_USAGE_ALLOWANCE", s));

//        REAL_REVENUE
        Optional<String> realRevenue = tx.getRealRevenue();
        realRevenue.ifPresent(s -> record.put("REAL_REVENUE", s));

//        START_TIME_OF_BILL_CYCLE
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("START_TIME_OF_BILL_CYCLE", s));

//        GROUP_PAY_FLAG
        Optional<String> groupPayFlag = tx.getGroupPayFlag();
        groupPayFlag.ifPresent(s -> record.put("GROUP_PAY_FLAG", s));

//        SERVED_MSISDN , OTHER_MSISDN
        String serviceFlow = tx.getValue("ServiceFlow");
        if (serviceFlow != null) {
            record.put("SERVICE_FLOW", serviceFlow);
            if ("1".equals(serviceFlow.trim())) {
                String callingPartyNumber = tx.getValue("CallingPartyNumber");
                String servedMSISDN = tx.normalizeMSISDN(callingPartyNumber);
                record.put("SERVED_MSISDN", servedMSISDN);
                try {
                    ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(servedMSISDN);
                    if (ddk != null) {
                        record.put("SERVED_MSISDN_DIALED_KEY", ddk.getDialDigitKey());
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                }

                String calledPartyNumber = tx.getValue("CalledPartyNumber");
                String otherMSISDN = tx.normalizeMSISDN(calledPartyNumber);
                record.put("OTHER_MSISDN", otherMSISDN);
                try {
                    ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(otherMSISDN);
                    if (ddk != null) {
                        record.put("OTHER_MSISDN_DIALED_KEY", ddk.getDialDigitKey());
                        String providerDesc = ddk.getProviderDesc();
                        String isoCountryCode = ddk.getIsoCountryCode();
                        record.put("OTHER_PARTY_ISO", isoCountryCode);
                        record.put("OTHER_PARTY_OPERATOR", providerDesc);
                        String targetCountryCode = ddk.getTargetCountryCode();
                        String otherPartyNwIndKey = "";
                        if (targetCountryCode.equals("966")) {
                            switch (providerDesc) {
                                case "GSM-Lebara Mobile":
                                case "LEBARA- Free Number":
                                case "LEBARA-Spl Number":
                                case "LEBARA-VAS":
                                case "LEBARA -M2M":
                                case "LEBARA-B2B-DATA-NUMBER":
                                    otherPartyNwIndKey = "1";
                                    break;
                                default:
                                    otherPartyNwIndKey = "2";
                                    break;
                            }
                        } else if (!targetCountryCode.equals("966")) {
                            otherPartyNwIndKey = "3";
                        } else {
                            otherPartyNwIndKey = "-99";
                        }
                        record.put("OTHER_PARTY_NW_IND_KEY", otherPartyNwIndKey);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                record.put("SERVED_IMSI", tx.getValue("CallingPartyIMSI"));
                record.put("OTHER_IMSI", tx.getValue("CalledPartyIMSI"));
                record.put("SERVED_CUG", tx.getValue("CallingCUGNo"));
                record.put("OTHER_CUG", tx.getValue("CalledCUGNo"));
                record.put("SERVED_ROAM_INFO", tx.getValue("CallingRoamInfo"));
                record.put("SERVED_CELL", tx.getValue("CallingCellID"));
                record.put("OTHER_PLMN", tx.getValue("CalledRoamInfo"));
                record.put("OTHER_CELL", tx.getValue("CalledCellID"));
                record.put("SERVED_CC", tx.getValue("CallingHomeCountryCode"));
                record.put("SERVED_AREA_CODE", tx.getValue("CallingHomeAreaNumber"));
                record.put("SERVED_NETWORK_CODE", tx.getValue("CallingHomeNetworkCode"));
                record.put("SERVED_ROAMING_CC", tx.getValue("CallingRoamCountryCode"));
                record.put("SERVED_ROAM_AREA_CODE", tx.getValue("CallingRoamAreaNumber"));
                record.put("SERVED_ROAM_NETWORK_CODE", tx.getValue("CallingRoamNetworkCode"));
                record.put("OTHER_CC", tx.getValue("CalledHomeCountryCode"));
                record.put("OTHER_AREA_CODE", tx.getValue("CalledHomeAreaNumber"));
                record.put("OTHER_NETWORK_CODE", tx.getValue("CalledHomeNetworkCode"));
                record.put("OTHER_ROAM_CC", tx.getValue("CalledRoamCountryCode"));
                record.put("OTHER_ROAM_AREA_CODE", tx.getValue("CalledRoamAreaNumber"));
                record.put("OTHER_ROAM_NETWORK_CODE", tx.getValue("CalledRoamNetworkCode"));
                record.put("SERVED_NW_TYPE", tx.getValue("CallingNetworkType"));
                record.put("OTHER_NW_TYPE", tx.getValue("CalledNetworkType"));
                record.put("SERVED_VPN_TOP_GROUP_NUMBER", tx.getValue("CallingVPNTopGroupNumber"));
                record.put("SERVED_VPN_GROUP_NUMBER", tx.getValue("CallingVPNGroupNumber"));
                record.put("SERVED_VPN_SHORT_NUMBER", tx.getValue("CallingVPNShortNumber"));
                record.put("OTHER_VPN_TOP_GROUP_NUMBER", tx.getValue("CalledVPNTopGroupNumber"));
                record.put("OTHER_VPN_GROUP_NUMBER", tx.getValue("CalledVPNGroupNumber"));
                record.put("OTHER_VPN_SHORT_NUMBER", tx.getValue("CalledVPNShortNumber"));
            } else if ("2".equals(serviceFlow.trim())) {
                String callingPartyNumber1 = tx.getValue("CallingPartyNumber");
                String calledPartyNumber1 = tx.getValue("CalledPartyNumber");
                String servedMSISDN = tx.normalizeMSISDN(calledPartyNumber1);
                String otherMSISDN1 = tx.normalizeMSISDN(callingPartyNumber1);
                record.put("SERVED_MSISDN", servedMSISDN);
                record.put("OTHER_MSISDN", otherMSISDN1);

                try {
                    ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(servedMSISDN);
                    if (ddk != null) {
                        record.put("SERVED_MSISDN_DIALED_KEY", ddk.getDialDigitKey());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    ReferenceDimDialDigit ddk = tx.getDialedDigitSettings(otherMSISDN1);
                    if (ddk != null) {
                        record.put("OTHER_MSISDN_DIALED_KEY", ddk.getDialDigitKey());
                        String isoCountryCode = ddk.getIsoCountryCode();
                        record.put("OTHER_PARTY_ISO", isoCountryCode);
                        String providerDesc = ddk.getProviderDesc();
                        record.put("OTHER_PARTY_OPERATOR", providerDesc);
                        String targetCountryCode = ddk.getTargetCountryCode();
                        String otherPartyNwIndKey = "";
                        if (targetCountryCode.equals("966")) {
                            switch (providerDesc) {
                                case "GSM-Lebara Mobile":
                                case "LEBARA- Free Number":
                                case "LEBARA-Spl Number":
                                    otherPartyNwIndKey = "1";
                                    break;
                                default:
                                    otherPartyNwIndKey = "2";
                                    break;
                            }
                        } else if (!targetCountryCode.equals("966")) {
                            otherPartyNwIndKey = "3";
                        } else {
                            otherPartyNwIndKey = "-99";
                        }
                        record.put("OTHER_PARTY_NW_IND_KEY", otherPartyNwIndKey);
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                }
                record.put("SERVED_IMSI", tx.getValue("CalledPartyIMSI"));
                record.put("OTHER_IMSI", tx.getValue("CallingPartyIMSI"));
                record.put("SERVED_CUG", tx.getValue("CalledCUGNo"));
                record.put("OTHER_CUG", tx.getValue("CallingCUGNo"));
                record.put("SERVED_NW_TYPE", tx.getValue("CalledNetworkType"));
                record.put("OTHER_NW_TYPE", tx.getValue("CallingNetworkType"));
                record.put("SERVED_PLMN", tx.getValue("CalledRoamInfo"));
                record.put("SERVED_CELL", tx.getValue("CalledCellID"));
                record.put("OTHER_PLMN", tx.getValue("CallingRoamInfo"));
                record.put("OTHER_CELL", tx.getValue("CallingCellID"));
                record.put("SERVED_CC", tx.getValue("CalledHomeCountryCode"));
                record.put("SERVED_AREA_CODE", tx.getValue("CalledHomeAreaNumber"));
                record.put("SERVED_NETWORK_CODE", tx.getValue("CalledHomeNetworkCode"));
                record.put("SERVED_ROAMING_CC", tx.getValue("CalledRoamCountryCode"));
                record.put("SERVED_ROAM_AREA_CODE", tx.getValue("CalledRoamAreaNumber"));
                record.put("SERVED_ROAM_NETWORK_CODE", tx.getValue("CalledRoamNetworkCode"));
                record.put("OTHER_CC", tx.getValue("CallingHomeCountryCode"));
                record.put("OTHER_AREA_CODE", tx.getValue("CallingHomeAreaNumber"));
                record.put("OTHER_NETWORK_CODE", tx.getValue("CallingHomeNetworkCode"));
                record.put("OTHER_ROAM_CC", tx.getValue("CallingRoamCountryCode"));
                record.put("OTHER_ROAM_AREA_CODE", tx.getValue("CallingRoamAreaNumber"));
                record.put("OTHER_ROAM_NETWORK_CODE", tx.getValue("CallingRoamNetworkCode"));
                record.put("SERVED_VPN_TOP_GROUP_NUMBER", tx.getValue("CalledVPNTopGroupNumber"));
                record.put("SERVED_VPN_GROUP_NUMBER", tx.getValue("CalledVPNGroupNumber"));
                record.put("SERVED_VPN_SHORT_NUMBER", tx.getValue("CalledVPNShortNumber"));
                record.put("OTHER_VPN_TOP_GROUP_NUMBER", tx.getValue("CallingVPNTopGroupNumber"));
                record.put("OTHER_VPN_GROUP_NUMBER", tx.getValue("CallingVPNGroupNumber"));
                record.put("OTHER_VPN_SHORT_NUMBER", tx.getValue("CallingVPNShortNumber"));
            }
        }

//        CHARGE, ZERO_CHRG_IND
        Optional<String> charge = tx.getCharge("DEBIT_AMOUNT");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHARGE_IND", s.equals("0") ? "1" : "0");
        });

//        ZeroDurationInd
        AtomicInteger zeroDurationIndDefault = new AtomicInteger(1);
        Optional<String> zeroDurationInd = tx.getZeroDurationInd();
        zeroDurationInd.ifPresent(s -> {
            if (!s.equals("0")) zeroDurationIndDefault.set(0);
        });
        record.put("ZERO_DURATION_IND", zeroDurationIndDefault.get());

//        OFFER_NAME , PAY_MODE , OFFER_TYPE_BI , SERVED_TYPE
        String offerid = tx.getValue("MainOfferingID");
        String servedType = null;
        String usageServiceType = tx.getValue("USAGE_SERVICE_TYPE");
        ReferenceDimCbsOfferPayType offeringInfo = LebaraUtil.getDimcbsOfferId(offerid);

        if (offeringInfo != null) {
            record.put("OFFER_NAME", offeringInfo.getOfferName());
            record.put("PAY_MODE", offeringInfo.getPayMode());
            record.put("OFFER_TYPE_BI", offeringInfo.getOfferTypeBi());
            String payMode = offeringInfo.getPayMode();
            String offerType = offeringInfo.getOfferTypeBi();

            if (usageServiceType.equals("15")) {
                if (payMode.equals("0") && offerType.equalsIgnoreCase("PREPAID")) {
                    servedType = "6";
                } else if (payMode.equals("0") && offerType == null) {
                    servedType = "6";
                } else if (payMode.equals("1") && offerType.equalsIgnoreCase("POSTPAID")) {
                    servedType = "5";
                } else if (payMode.equals("1") && offerType == null) {
                    servedType = "5";
                } else if (payMode != null && offerType.equalsIgnoreCase("POSTPAID2")) {
                    servedType = "2";
                } else if (payMode.equals("3") && offerType.equalsIgnoreCase("HYBRID")) {
                    servedType = "8";
                } else if (payMode.equals("3") && offerType == null) {
                    servedType = "8";
                }
            }
            else {
                if (payMode.equals("0") && offerType.equalsIgnoreCase("PREPAID")) {
                    servedType = "2";
                } else if (payMode.equals("0") && offerType == null) {
                    servedType = "2";
                } else if (payMode.equals("1") && offerType.equalsIgnoreCase("POSTPAID")) {
                    servedType = "1";
                } else if (payMode.equals("1") && offerType == null) {
                    servedType = "1";
                } else if (payMode != null && offerType.equalsIgnoreCase("POSTPAID2")) {
                    servedType = "1";
                } else if (payMode.equals("3") && offerType.equalsIgnoreCase("HYBRID")) {
                    servedType = "7";
                } else if (payMode.equals("3") && offerType == null) {
                    servedType = "8";
                }
            }
            record.put("SERVED_TYPE" , servedType);
        }

//        SERVICE_FLOW
        record.put("SERVICE_FLOW", serviceFlow);

//        FILE_NAME , POPULATION_DATE , EVENT_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));
        record.put("EVENT_DATE", tx.genFullDate);

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
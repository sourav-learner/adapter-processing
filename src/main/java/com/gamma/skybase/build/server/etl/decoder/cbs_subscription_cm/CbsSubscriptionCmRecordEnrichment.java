package com.gamma.skybase.build.server.etl.decoder.cbs_subscription_cm;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimCbsOfferPayType;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class CbsSubscriptionCmRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsSubscriptionCmEnrichmentUtil tx = CbsSubscriptionCmEnrichmentUtil.of(record);

        //  CDR_STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("CDR_STATUS", s));

        // EVENT_START_TIME
        Optional<String> starTime = tx.getStartTime("CUST_LOCAL_START_DATE");
        starTime.ifPresent(s -> {
            record.put("EVENT_START_TIME", s);
            record.put("XDR_DATE", s);
        });

        // EVENT_END_TIME
        Optional<String> endTime = tx.getEndTime("CUST_LOCAL_END_DATE");
        endTime.ifPresent(s -> record.put("EVENT_END_TIME", s));

        // OBJTYPE
        Optional<String> objType = tx.getObjType();
        objType.ifPresent(s -> record.put("OBJTYPE", s));

        // RESULTCODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

//      SERVICE_CATEGORY
        String serviceCategory = tx.getValue("SERVICE_CATEGORY");
        record.put("SERVICE_CATEGORY1",serviceCategory);

        // PAY_TYPE
        String payType = tx.getValue("PayType");
        record.put("PAY_TYPE", payType);

        // SERVED_MSISDN
        String callingPartyNumber = tx.getValue("CallingPartyNumber");
        if(callingPartyNumber != null) {
            String servedMsisdn;
            if (callingPartyNumber.length()<12){
                servedMsisdn = "966" + callingPartyNumber;
            }
            else {
                servedMsisdn = callingPartyNumber;
            }
            record.put("SERVED_MSISDN",servedMsisdn);
        }

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

        // FILE_NAME , POPULATION_DATE , EVENT_DATE
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
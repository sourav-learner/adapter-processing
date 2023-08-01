package com.gamma.skybase.build.server.etl.decoder.cbs_gprs;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimCbsOfferPayType;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class CbsGprsRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsGprsEnrichmentUtil tx = CbsGprsEnrichmentUtil.of(record);

        //  STATUS
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

        // CHARGING_TIME
        Optional<String> chargingTime = tx.getChargingTime("ChargingTime");
        chargingTime.ifPresent(s -> record.put("CHARGING_TIME", s));

        //  ONLINE_CHARGING_FLAG
        Optional<String> onlineChargingFlag = tx.getOnlineChargingFlag();
        onlineChargingFlag.ifPresent(s -> record.put("ONLINE_CHARGING_FLAG", s));

        //  START_TIME_OF_BILL_CYCLE
        Optional<String> startTimeOfBill = tx.getStartTimeOfBillCycle("StartTimeOfBillCycle");
        startTimeOfBill.ifPresent(s -> record.put("START_TIME_OF_BILL_CYCLE", s));

        // CHARGE, ZERO_CHRG_IND
        Optional<Double> charge = tx.getCharge("DEBIT_AMOUNT");
        charge.ifPresent(s -> {
            record.put("CHARGE", s);
            record.put("ZERO_CHARGE_IND", s == 0 ? "1" : "0");
        });

        // UploadVolume
        Optional<Long> uploadVolume = tx.getUploadVolume("UpFlux");
        uploadVolume.ifPresent(s -> record.put("COMPUTED_UPLOAD_VOL", s));
        String UpFlux = tx.getValue("UpFlux");
        record.put("UpFlux", UpFlux);

        // DownloadVolume
        Optional<Long> downloadVolume = tx.getDownloadVolume("DownFlux");
        downloadVolume.ifPresent(s -> record.put("COMPUTED_DOWNLOAD_VOL", s));
        String DownFlux = tx.getValue("DownFlux");
        record.put("DownFlux", DownFlux);

        // TotalFlux
        Optional<Long> totalFlux = tx.getTotalVol("TotalFlux");
        totalFlux.ifPresent(s -> record.put("COMPUTED_TOTAL_VOL", s));
        String TotalFlux = tx.getValue("TotalFlux");
        record.put("TotalFlux", TotalFlux);

        //        CHARGING_PARTY_NUMBER
        Optional<String> chargingPartyNumber = tx.getChargingPartyNumber();
        chargingPartyNumber.ifPresent(s -> record.put("CHARGING_PARTY_NUMBER", s));

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

//        SERVED_MSISDN
        String callingPartyNumber = tx.getValue("CallingPartyNumber");
        String servedMSISDN = tx.normalizeMSISDN(callingPartyNumber);
        record.put("SERVED_MSISDN", servedMSISDN);

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

//        FILE_NAME , POPULATION_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        // EVENT_DATE
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
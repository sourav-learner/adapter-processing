package com.gamma.skybase.build.server.etl.tx.mmsc;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;

public class MMSCRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(MMSCRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final String opcoName = AppConfig.instance().getProperty("app.datasource.opconame");
    private final String isoCode = AppConfig.instance().getProperty("app.datasource.isocode");
    private final String operatorCode = AppConfig.instance().getProperty("app.datasource.opcode");
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final DecimalFormat decimalFormat = new DecimalFormat("#.000000");

    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else response.setResponseCode(false);
        return response;
    }

    @Override
    @SuppressWarnings("Duplicates")
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();
        try {
            //"MESSAGE_ID", "CALLER_ID", "CALLER_ID_IMSI", "SENDER", "SENDER_IMSI", "RECIPIENT",
            //                "RECIPIENT_IMSI", "MESSAGE_SIZE", "SUBMISSION_TIME_STAMP", "EARLIEST_DELIVERY_TIMESTAMP", "EXPIRATION_TIME_STAMP", "UNKNOWN_1",
            //                "MESSAGE_TYPE", "BEARER_TYPE", "CONTENT_TYPE", "MESSAGE_CLASS", "SENDER_HIDE_REQUESTED", "DELIVERY_REPORT_REQUESTED",
            //                "READ_REPLY_REQUESTED", "STORAGE_DURATION", "PARTY_TO_BILL", "MM7_SERVICE_CODE", "MM_STATUS", "FORWARDING_INDICATION",
            //                "CONVERSION_OF_MEDIA_TYPES", "PRIORITY", "VASPID", "VASID", "GPRS_USERNAME", "GPRS_CHARGING_ID",
            //                "PREPAID_OR_NOT", "GGSN", "SGSN", "CHARGING_INDICATOR"
            record.put("MESSAGE_ID", data.get("MESSAGE_ID"));
            record.put("CALLER_ID", data.get("CALLER_ID"));
            record.put("CALLER_ID_IMSI", data.get("CALLER_ID_IMSI"));
            record.put("SENDER", data.get("SENDER"));
            record.put("SENDER_IMSI", data.get("SENDER_IMSI"));
            record.put("RECIPIENT", data.get("RECIPIENT"));
            record.put("RECIPIENT_IMSI", data.get("RECIPIENT_IMSI"));
//            record.put("MESSAGE_SIZE", data.get("MESSAGE_SIZE"));
            record.put("MESSAGE_TYPE", data.get("MESSAGE_TYPE"));
            record.put("BEARER_TYPE", data.get("BEARER_TYPE"));
            record.put("CONTENT_TYPE", data.get("CONTENT_TYPE"));
            record.put("MESSAGE_CLASS", data.get("MESSAGE_CLASS"));
            record.put("SENDER_HIDE_REQUESTED", data.get("SENDER_HIDE_REQUESTED"));
            record.put("DELIVERY_REPORT_REQUESTED", data.get("DELIVERY_REPORT_REQUESTED"));
            record.put("READ_REPLY_REQUESTED", data.get("READ_REPLY_REQUESTED"));
            record.put("MESSAGE_STATUS", data.get("MM_STATUS"));
            record.put("FORWARDING_INDICATION", data.get("FORWARDING_INDICATION"));
            record.put("CONVERSION_OF_MEDIA_TYPES", data.get("CONVERSION_OF_MEDIA_TYPES"));
            record.put("PRIORITY", data.get("PRIORITY"));
            record.put("VASPID", data.get("VASPID"));
            record.put("VASID", data.get("VASID"));
            record.put("GPRS_USERNAME", data.get("GPRS_USERNAME"));
            record.put("GPRS_CHARGING_ID", data.get("GPRS_CHARGING_ID"));
            record.put("PREPAID_OR_NOT", data.get("PREPAID_OR_NOT"));
            record.put("GGSN_TYPE", data.get("GGSN"));
            record.put("SGSN_TYPE", data.get("SGSN"));

            String sender = String.valueOf(data.get("SENDER"));
            String recipient = String.valueOf(data.get("RECIPIENT"));
            String servedImsi = String.valueOf(data.get("SENDER_IMSI"));
            String charge = String.valueOf(data.get("CHARGING_INDICATOR"));
            String messageSize = String.valueOf(data.get("MESSAGE_SIZE"));
            String mmStatus = String.valueOf(data.get("MM_STATUS"));

            String submissionTimeStamp = String.valueOf(data.get("SUBMISSION_TIME_STAMP"));
            String earliestDeliveryTimestamp = String.valueOf(data.get("EARLIEST_DELIVERY_TIMESTAMP"));
            String expirationTimeStamp = String.valueOf(data.get("EXPIRATION_TIME_STAMP"));

            long mSize = 0;
            if (StringUtils.isNotEmpty(messageSize) && StringUtils.isNumeric(messageSize)){
                mSize = Long.parseLong(messageSize);
            }
            record.put("MESSAGE_SIZE", mSize);

            if (StringUtils.isNotEmpty(mmStatus)){
                mmStatus = mmStatus.toLowerCase();
            }
            record.put("MESSAGE_STATUS", mmStatus);

            String servedMsisdn, otherMsisdn;
            if (StringUtils.isNumeric(sender)) servedMsisdn = sender;
            else {
                if (sender.contains("/")) servedMsisdn = sender.substring(0, sender.lastIndexOf("/"));
                else servedMsisdn = sender;
            }

            if (StringUtils.isNumeric(recipient)) otherMsisdn = recipient;
            else {
                if (recipient.contains("/")) otherMsisdn = recipient.substring(0, recipient.lastIndexOf("/"));
                else otherMsisdn = recipient;
            }

            record.put("SERVED_MSISDN", servedMsisdn);
            record.put("OTHER_MSISDN", otherMsisdn);

            String servedOper, otherOper;


            ReferenceDimDialDigit otherDialDigit = transformationLib.getDialedDigitSettings(otherMsisdn);

            if (StringUtils.isNumeric(servedMsisdn)) {
                ReferenceDimDialDigit servedDialDigit = transformationLib.getDialedDigitSettings(servedMsisdn);
                record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", servedDialDigit.getDialDigitKey());
                record.put("SERVED_MSISDN_ISO_CODE", servedDialDigit.getIsoCountryCode());
                servedOper = servedDialDigit.getProviderDesc();
                if (servedOper != null && servedOper.trim().length() > 0) {
                    record.put("SERVED_OPER", servedOper);
                }
            } else {
                record.put("SERVED_MSISDN_DIAL_DIGIT_KEY", countryCode);
                record.put("SERVED_MSISDN_ISO_CODE", isoCode);
                record.put("SERVED_OPER", opcoName);
            }

            if  (otherDialDigit != null) {
                record.put("OTHER_MSISDN_DIAL_DIGIT_KEY", otherDialDigit.getDialDigitKey());
                record.put("OTHER_MSISDN_NOP_ID_KEY", otherDialDigit.getNopIdKey());
                record.put("EVENT_CATEGORY_KEY", otherDialDigit.getEventCategoryKey());
                record.put("OTHER_MSISDN_ISO_CODE", otherDialDigit.getIsoCountryCode());
                record.put("OTHER_MSISDN_DEST_TYPE", otherDialDigit.getDialDigitDesc());
                otherOper = otherDialDigit.getProviderDesc();
                if (otherOper != null && otherOper.trim().length() > 0) {
                    record.put("OTHER_OPER", otherOper);
                }

                String nwIndKey;
                if ("DOMESTIC".equalsIgnoreCase(otherDialDigit.getDialDigitDesc())) {
                    if ("125".equalsIgnoreCase(otherDialDigit.getNopIdKey())) nwIndKey = "1";
                    else nwIndKey = "2";
                } else {
                    nwIndKey = "3";
                }
                record.put("NW_IND_KEY", nwIndKey);
            }

            String srvTypeKey = null;
            if (servedImsi != null) {
                if (!servedImsi.startsWith(operatorCode)) {
                    srvTypeKey  = "3";
                }
            }

            if (srvTypeKey == null) {
//                Object prepaidOrNot = data.get("PREPAID_OR_NOT");
//                if (prepaidOrNot != null) {
//                    srvTypeKey = "N".equalsIgnoreCase(String.valueOf(prepaidOrNot)) ? "1" :  "2";
//                }
                srvTypeKey = getSrvTypeKeyBySubscriber(servedMsisdn);
            }
            record.put("SRV_TYPE_KEY", srvTypeKey);

            if (!charge.isEmpty()) {
                String messageCharge = decimalFormat.format(Double.parseDouble(charge));
                record.put("CHARGE", messageCharge);
            }
            record.put("SERVED_IMSI", servedImsi);

            if (!submissionTimeStamp.isEmpty()) {
                Date recordTimestamp = sourceDateFormat.get().parse(submissionTimeStamp);
                record.put("SUBMISSION_TIMESTAMP", targetDateFormat.get().format(recordTimestamp));
                record.put("XDR_DATE", targetDateFormat.get().format(recordTimestamp));
            }

            if (!earliestDeliveryTimestamp.isEmpty()) {
                Date earliest = sourceDateFormat.get().parse(earliestDeliveryTimestamp);
                record.put("EARLIEST_DELIVERY_TIMESTAMP", targetDateFormat.get().format(earliest));
            }

            if (!expirationTimeStamp.isEmpty()) {
                Date expiration = sourceDateFormat.get().parse(expirationTimeStamp);
                record.put("EXPIRATION_TIMESTAMP", targetDateFormat.get().format(expiration));
            }
            record.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));

//            record.put("FILE_NAME", data.get("fileName"));
        } catch (Exception e) {
            logger.error("Abort !!! Exception parsing record --> " + e.getMessage(), e);
        }
        return record;
    }

    private String getSrvTypeKeyBySubscriber(String msisdn){
        String srvTypeKey = "2";
        if (StringUtils.isNotEmpty(msisdn)){
            String postpaid = transformationLib.getDimLookupCRMSubscriber(msisdn);
            srvTypeKey = postpaid == null ? "2" : "1";
        }
        return srvTypeKey;
    }
}

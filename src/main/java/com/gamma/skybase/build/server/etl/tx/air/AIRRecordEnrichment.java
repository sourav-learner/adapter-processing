package com.gamma.skybase.build.server.etl.tx.air;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.server.etl.utils.OpcoBusinessTransformation;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by abhi on 2019-04-24
 */
public class AIRRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(AIRRecordEnrichment.class);

    private static final String FORMAT1 = "yyyyMMdd";
    private static final String FORMAT2 = "yyyyMMdd HH";
    private static final String FORMAT3 = "yyyyMMddHHmmss";
    private static final String FORMAT4 = "yyyyMMdd HH:mm:ss";

    private static final ThreadLocal<SimpleDateFormat> yyyyMMdd = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT1));
    private static final ThreadLocal<SimpleDateFormat> yyyyMMddHH = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT2));
    private static final ThreadLocal<SimpleDateFormat> yyyyMMddHHmmss = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT3));
    private static final ThreadLocal<SimpleDateFormat> yyyyMMddFormatted = ThreadLocal.withInitial(() -> new SimpleDateFormat(FORMAT4));

    private final DecimalFormat monetaryFormat = new DecimalFormat("#.######");;
    private static final AppConfig appConfig = AppConfig.instance();
    private final String countryCode = appConfig.getProperty("app.datasource.countrycode");
    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

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


    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {

        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
        txRecord.put("eventType", record.get("eventType"));
        txRecord.put("EVENT_TYPE", record.get("eventType"));
        Object eventType = record.get("eventType");
        if (eventType != null)
            switch (eventType.toString().trim()) {
                case "AdjustmentRecordV2":
                    txRecord.putAll(handleAdjustmentRecords(record));
                    break;
                case "RefillRecordV2":
                    txRecord.putAll(handleRefillRecords(record));
                    break;
                case "OfflinePromotionRecordV2":
                    handleOfflinePromotionRecords(record);
                    break;
                case "ErrorRecordV2":
                    txRecord.putAll(handleErrorRecords(record));
                    break;
                case "CommunicationIDChangeRecord":
                    break;
                default:
                    logger.info("!!! Not a valid Event Type !!!");
                    break;
            }
        else logger.info("!!! Not a valid Event Type !!!");

        try {
            txRecord.put("FILE_NAME", record.get("fileName"));
            Object originTimeStamp = record.get("originTimeStamp");
            Object timeStamp = record.get("timeStamp");
            try {
                String formattedOrgTs = handleDateObject(originTimeStamp.toString());
                txRecord.put("originTimeStamp", formattedOrgTs);

                Date ts = yyyyMMddHHmmss.get().parse(timeStamp.toString());
                timeStamp = yyyyMMddFormatted.get().format(ts);
                txRecord.put("timeStamp", timeStamp);

                txRecord.put("GENERATED_FULL_DATE", yyyyMMdd.get().format(ts) + " 00:00:00");
                txRecord.put("EVENT_START_TIME_SLOT_KEY", yyyyMMddHH.get().format(ts) + ":00:00");
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            txRecord.put("EVENT_START_TIME", timeStamp);
            txRecord.put("XDR_DATE", timeStamp);
            txRecord.put("POPULATION_DATE_TIME", yyyyMMddFormatted.get().format(new Date()));

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        /*Trim values if they are string*/
        for(Map.Entry<String,Object> entry : txRecord.entrySet()){
            if(entry.getValue() instanceof  String){
                entry.setValue(((String)entry.getValue()).trim());
            }
        }
        return txRecord;

    }

    private LinkedHashMap<String, Object> handleRefillRecords(LinkedHashMap<String, Object> record) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
        Object originNodeType = record.get("originNodeType");
        Object originHost = record.get("originHostName");
        Object hostName = record.get("hostName");
        Object originTransactionID = record.get("originTransactionID");
        Object localSequenceNumber = record.get("localSequenceNumber");
        Object currentServiceClass = record.get("currentServiceClass");
        Object voucherBasedRefill = record.get("voucherBasedRefill");
        Object transactionAmount = record.get("transactionAmount");
        Object transactionCurrency = record.get("transactionCurrency");
        Object refillType = record.get("refillType");
        Object segmentationID = record.get("segmentationID");
        Object voucherSerialNumber = record.get("voucherSerialNumber");
        Object voucherGroupID = record.get("voucherGroupID");
        Object subscriberNumber = record.get("subscriberNumber");
        Object activationDate = record.get("activationDate");
        Object voucherAgent = record.get("voucherAgent");
        Object accountGroupId = record.get("accountGroupId");
        Object requestedRefillType = record.get("requestedRefillType");
        Object externalData1 = record.get("externalData1");
        Object externalData2 = record.get("externalData2");
        Object accountHomeRegion = record.get("accountHomeRegion");

        if (originNodeType != null) txRecord.put("ORIGIN_NODE_TYPE", originNodeType);
        if (originHost != null) txRecord.put("originHostName", originHost);
        if (hostName != null) txRecord.put("HOST_NAME", hostName);
        if (originTransactionID != null) txRecord.put("TRANSACTION_ID", originTransactionID);
        if (localSequenceNumber != null) txRecord.put("EDR_SEQ_NUM", localSequenceNumber);
        if (currentServiceClass != null) txRecord.put("SERVICE_CLASS", currentServiceClass);
        if (voucherBasedRefill != null) txRecord.put("VOUCHER_BASED_REFILL", voucherBasedRefill);
        if (transactionAmount != null) {
            txRecord.put("TRANSACTION_AMOUNT", transactionAmount);
            if (transactionAmount instanceof Double){
                txRecord.put("DENOMINATION", ((Double) transactionAmount).intValue());
            } else if (transactionAmount instanceof String){
                try{
                    txRecord.put("DENOMINATION", ((Double) Double.parseDouble((String) transactionAmount)).intValue());
                }catch (Exception e){
                    txRecord.put("DENOMINATION", 0);
                }
            }
        }
        if (transactionCurrency != null) txRecord.put("TRANSACTION_CURRENCY", transactionCurrency);
        if (refillType != null) txRecord.put("REFILL_TYPE", refillType);
        if (segmentationID != null) txRecord.put("SEGMENTATION_ID", segmentationID);
        if (voucherSerialNumber != null) txRecord.put("VOUCHER_SERIAL", voucherSerialNumber);
        if (voucherGroupID != null) txRecord.put("VOUCHER_GROUP_ID", voucherGroupID);
        if (subscriberNumber != null) txRecord.put("SUBSCRIBER_NUMBER", subscriberNumber);
        if (activationDate != null) txRecord.put("ACTIVATION_DATE", activationDate);
        if (voucherAgent != null) txRecord.put("VOUCHER_AGENT", voucherAgent);
        if (accountGroupId != null) txRecord.put("ACCOUNT_GROUP_ID", accountGroupId);
        if (requestedRefillType != null) txRecord.put("REQ_REFILL_TYPE", requestedRefillType);
        if (accountHomeRegion != null) txRecord.put("ACCOUNT_HOME_REGION", accountHomeRegion);
        if (externalData1 != null) txRecord.put("EXTERNAL_DATA1", externalData1);
        if (externalData2 != null) txRecord.put("EXTERNAL_DATA2", externalData2);

        if (originNodeType !=  null && originHost != null) {
            String voucherSerial = voucherSerialNumber == null  ? "" : voucherSerialNumber.toString().trim();
            String fileName = record.get("fileName") == null  ? "" : record.get("fileName").toString().trim();
            String ext1 = externalData1 == null  ? "" : externalData1.toString().trim();
            String ext2 = externalData2 == null  ? "" : externalData2.toString().trim();
            txRecord.put("REFILL_RECORD_TYPE", getAirRefillRecordType(originNodeType.toString(),
                    originHost.toString(), voucherSerial, ext1, ext2, fileName));
        }

        String accountNumber = record.get("accountNumber").toString();
        String servedMsisdn = "";
        if(accountNumber != null) {
            if(!accountNumber.isEmpty()){
                servedMsisdn += countryCode;
                servedMsisdn += accountNumber;
            }
        }
        txRecord.put("ACCOUNT_NUMBER", accountNumber);
        txRecord.put("SERVED_MSISDN", servedMsisdn);

        Object accountInformationBeforeRefill = record.get("accountInformationBeforeRefill");
        Object accountInformationAfterRefill = record.get("accountInformationAfterRefill");
        LinkedHashMap<String, Object> accountInformation = handleRefillAccountInformation(accountInformationBeforeRefill,
                accountInformationAfterRefill);

        if (!accountInformation.isEmpty()) txRecord.putAll(accountInformation);
        return txRecord;
    }

    private String getAirRefillRecordType(String originNodeType, String originHostName, String voucherSerial,
                                          String externalData1, String externalData2, String fileName) {
        String refillRecordType = "";
        originNodeType = originNodeType.trim();
        originHostName = originHostName.trim();
        if ("EXT".equalsIgnoreCase(originNodeType) && "USSDGWIAXF".equalsIgnoreCase(originHostName)) refillRecordType = "1";
        if ("UGW".equalsIgnoreCase(originNodeType)) refillRecordType = "1";
        if ("EXT".equalsIgnoreCase(originNodeType) && "EVD".equalsIgnoreCase(originHostName)) refillRecordType = "2";
        if ("AIR".equalsIgnoreCase(originNodeType) && "ZPP".equalsIgnoreCase(originHostName)) refillRecordType = "2";
        if ("EXT".equalsIgnoreCase(originNodeType) && "ESB".equalsIgnoreCase(originHostName)) refillRecordType = "1";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && "Corporate".equalsIgnoreCase(externalData2)) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && externalData2.contains("Center")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && "HR".equalsIgnoreCase(externalData2)) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && "hwatif".equalsIgnoreCase(externalData2)) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().contains("hawatif")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().contains("franchise")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().contains("outsource")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().contains("marketing research")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().contains("customer information")) refillRecordType = "2";
        if ("Operation_Refill".equalsIgnoreCase(externalData1) && String.valueOf(externalData2).toLowerCase().startsWith("req")) refillRecordType = "2";

        /* per new logic */
        if (refillRecordType.isEmpty() && !voucherSerial.trim().isEmpty()){
//            if (voucherSerial.startsWith("5") || voucherSerial.startsWith("6") || voucherSerial.startsWith("7")){
            if (voucherSerial.startsWith("5") || voucherSerial.startsWith("6")){
                refillRecordType  = "2";
            }else {
                refillRecordType  = "1";
            }
        }else {
            /* if no serial_number & operation is refill then EVoucher */
            if ("Operation_Refill".equalsIgnoreCase(externalData1)){
                refillRecordType  = "2";
            }
        }

        if (refillRecordType.isEmpty()) {
            logger.error("originNodeType - {}, originHostName - {}, fileName - {}", originNodeType,
                    originHostName, fileName);
            refillRecordType = "-99";
        }
        return refillRecordType;
    }

    @SuppressWarnings("unchecked")
    private LinkedHashMap<String, Object> handleRefillAccountInformation(Object beforeAccount, Object afterAccount) {
        LinkedHashMap<String, Object> accountStats = new LinkedHashMap<>();
        ArrayList<Object> accountInformationBeforeRefill = (ArrayList<Object>) beforeAccount;
        ArrayList<Object> accountInformationAfterRefill = (ArrayList<Object>) afterAccount;

        LinkedHashMap<String, Object> before = (LinkedHashMap<String, Object>) accountInformationBeforeRefill.get(0);
        LinkedHashMap<String, Object> after = (LinkedHashMap<String, Object>) accountInformationAfterRefill.get(0);

        Object accountFlagsBeforeObj = before.get("accountFlags");
        Object accountBalanceBeforeObj = before.get("accountBalance");
        Object accountBalanceAfterObj = after.get("accountBalance");
        Object usageAccumulatorBeforeObj = before.get("usageAccumulator");
        Object usageAccumulatorAfterObj = after.get("usageAccumulator");
        Object offersBeforeObj = before.get("offers");
        Object offersAfterObj = after.get("offers");

        if (accountFlagsBeforeObj != null) accountStats.put("ACCOUNT_FLAGS", accountFlagsBeforeObj);
        if (accountBalanceBeforeObj != null) accountStats.put("ACCOUNT_BAL_BEFORE", accountBalanceBeforeObj);
        if (accountBalanceAfterObj != null) accountStats.put("ACCOUNT_BAL_AFTER", accountBalanceAfterObj);

        List<String> accumulatorUsed = new ArrayList<>();
        if (usageAccumulatorBeforeObj != null && usageAccumulatorAfterObj != null) {
            ArrayList<LinkedHashMap<String, Object>> usageAccumulatorBefore = (ArrayList<LinkedHashMap<String, Object>>) usageAccumulatorBeforeObj;
            ArrayList<LinkedHashMap<String, Object>> usageAccumulatorAfter = (ArrayList<LinkedHashMap<String, Object>>) usageAccumulatorAfterObj;

            LinkedHashMap<String, Object> accumulatorBefore = usageAccumulatorBefore.get(0);
            LinkedHashMap<String, Object> accumulatorAfter = usageAccumulatorAfter.get(0);

            for (Object entry : (ArrayList<?>) accumulatorAfter.get("usageAccumulator")) {
                if (entry instanceof ArrayList && ((ArrayList<?>) entry).size() > 0) {
                    LinkedHashMap<String, Object> accum = ((ArrayList<LinkedHashMap<String, Object>>) entry).stream().findFirst().get();
//                    LinkedHashMap<String, Object> accum = (LinkedHashMap<String, Object>) entry;
                    BigInteger usageAccumulatorID = (BigInteger) accum.get("usageAccumulatorID");
                    BigInteger usageAccumulatorValue = (BigInteger) accum.get("usageAccumulatorValue");

                    for (Object beforeEntry : (ArrayList<?>) accumulatorBefore.get("usageAccumulator")) {
                        if (beforeEntry instanceof ArrayList && ((ArrayList<?>) beforeEntry).size() > 0) {
                            LinkedHashMap<String, Object> beforeAccum = ((ArrayList<LinkedHashMap<String, Object>>) beforeEntry).stream().findFirst().get();
                            BigInteger beforeUsageAccumulatorID = (BigInteger) beforeAccum.get("usageAccumulatorID");
                            BigInteger beforeUsageAccumulatorValue = (BigInteger) beforeAccum.get("usageAccumulatorValue");

                            if (usageAccumulatorID.equals(beforeUsageAccumulatorID)) {
                                if (!usageAccumulatorValue.equals(beforeUsageAccumulatorValue)) {
                                    accumulatorUsed.add(usageAccumulatorID.toString());
                                }
                            }
                        }
                    }
                }
            }
        }

        accountStats.put("ACCOUNT_ACCUMULATOR", StringUtils.join(accumulatorUsed, "|"));
        return accountStats;
    }


    @SuppressWarnings("unchecked")
    private Map<String, Object> handleAdjustmentsDedicatedRecords(Object dedicatedAccounts) {
        LinkedHashMap<String, Object> accountDetails = new LinkedHashMap<>();
        ArrayList<Object> accounts = (ArrayList<Object>) dedicatedAccounts;
        if (accounts.size() > 0) {
            LinkedHashMap<String, Object> account = (LinkedHashMap<String, Object>) accounts.stream().findFirst().get();
            for (Object entry : (ArrayList<?>) account.get("dedicatedAccounts")) {
                if (entry instanceof ArrayList && ((ArrayList<?>) entry).size() > 0) {
                    LinkedHashMap<String, Object> dedicatedAccount = ((ArrayList<LinkedHashMap<String, Object>>) entry)
                            .stream().findFirst().get();
                    String dedicatedAccountID = String.valueOf(dedicatedAccount.get("dedicatedAccountID"));
                    String dedicatedAccountUnit = String.valueOf(dedicatedAccount.get("dedicatedAccountUnit"));
                    String accountBalance = String.valueOf(dedicatedAccount.get("accountBalance"));
                    String accountExpiryDateBefore = String.valueOf(dedicatedAccount.get("accountExpiryDateBefore"));
                    String accountExpiryDateAfter = String.valueOf(dedicatedAccount.get("accountExpiryDateAfter"));

                    accountDetails.put("DEDICATED_ACCOUNT_ID", dedicatedAccountID);
                    accountDetails.put("DEDICATED_ACCOUNT_UNIT", dedicatedAccountUnit);
                    accountDetails.put("ACCOUNT_BALANCE", accountBalance);
                    accountDetails.put("ACCOUNT_EXPIRY_DATE_BEFORE", accountExpiryDateBefore);
                    accountDetails.put("ACCOUNT_EXPIRY_DATE_AFTER", accountExpiryDateAfter);
                }
            }
        }
        return accountDetails;
    }

    private LinkedHashMap<String, Object> handleAdjustmentRecords(LinkedHashMap<String, Object> record) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();

        String originNodeType = record.get("originNodeType") != null ? String.valueOf(record.get("originNodeType")) : "";
        String originHostName = record.get("originHostName") != null ? String.valueOf(record.get("originHostName")) : "";
        String originTransactionID = record.get("originTransactionID") != null ? String.valueOf(record.get("originTransactionID")) : "";
        String originOperatorID = record.get("originOperatorID") != null ? String.valueOf(record.get("originOperatorID")) : "";
        String localSequenceNumber = record.get("localSequenceNumber") != null ? String.valueOf(record.get("localSequenceNumber")) : "";
        String currentServiceClass = record.get("currentServiceClass") != null ? String.valueOf(record.get("currentServiceClass")) : "";
        String transactionType = record.get("transactionType") != null ? String.valueOf(record.get("transactionType")) : "";
        String transactionCode = record.get("transactionCode") != null ? String.valueOf(record.get("transactionCode")) : "";
        String transactionAmount = record.get("transactionAmount") != null ? String.valueOf(record.get("transactionAmount")) : "";
        String transactionCurrency = record.get("transactionCurrency") != null ? String.valueOf(record.get("transactionCurrency")) : "";
        String accountNumber = record.get("accountNumber") != null ? String.valueOf(record.get("accountNumber")) : "";
        String accountCurrency = record.get("accountCurrency") != null ? String.valueOf(record.get("accountCurrency")) : "";
        String accountBalance = record.get("accountBalance") != null ? String.valueOf(record.get("accountBalance")) : "";
        String accountFlagsBefore = record.get("accountFlagsBefore") != null ? String.valueOf(record.get("accountFlagsBefore")) : "";
        String accountFlagsAfter = record.get("accountFlagsAfter") != null ? String.valueOf(record.get("accountFlagsAfter")) : "";
        String accountGroupId = record.get("accountGroupId") != null ? String.valueOf(record.get("accountGroupId")) : "";
        String externalData1 = record.get("externalData1") != null ? String.valueOf(record.get("externalData1")) : "";
        String externalData2 = record.get("externalData2") != null ? String.valueOf(record.get("externalData2")) : "";
        String fileName = record.get("fileName") != null ? String.valueOf(record.get("fileName")) : "";

        txRecord.put("ORIGIN_NODE_TYPE", originNodeType);
        txRecord.put("ORIGIN_HOST_NAME", originHostName);
        txRecord.put("TRANSACTION_ID", originTransactionID);
        txRecord.put("ORIGIN_OPERATOR_ID", originOperatorID);
        txRecord.put("EDR_SEQ_NUM", localSequenceNumber);
        txRecord.put("SERVICE_CLASS", currentServiceClass);
        txRecord.put("TRANSACTION_TYPE", transactionType);
        txRecord.put("TRANSACTION_CODE", transactionCode);
        txRecord.put("TRANSACTION_CURRENCY", transactionCurrency);
        txRecord.put("ACCOUNT_CURRENCY", accountCurrency);
        txRecord.put("ACCOUNT_GROUP_ID", accountGroupId);

        String adjustmentType = getAdjustmentTypeKey(originNodeType, originHostName, externalData2, externalData1, fileName);
        txRecord.put("ADJUSTMENT_TYPE_KEY", adjustmentType);

        Map<String, String> subscriptionProps = getSubscriptionTypeKey(originHostName, externalData1, externalData2, transactionCode, adjustmentType);
        if (!subscriptionProps.isEmpty()) {
            txRecord.put("SUBSCRIPTION_TYPE_KEY", subscriptionProps.get("subscriptionRecKey"));
            txRecord.put("SUBSCRIPTION_SERVICE_NAME", subscriptionProps.get("serviceName"));
            if (subscriptionProps.get("subscriptionRecKey").equalsIgnoreCase("-98")) {
                logger.info("originHostName {}, externalData1 {}, externalData2 {}, fileName {}", originHostName,
                        externalData1, externalData2, fileName);
            }
        }

        Object temp1 = record.get("accountInformationBeforeRefill");
        Object temp2 = record.get("accountInformationAfterRefill");
        Object dedicatedAccounts = record.get("dedicatedAccounts");


        if (temp1 != null) logger.info("accountInformationBeforeRefill ({}) - {}", fileName, temp1);
        if (temp2 != null) logger.info("accountInformationAfterRefill ({}) - {}", fileName, temp2);
        if (dedicatedAccounts != null) txRecord.putAll(handleAdjustmentsDedicatedRecords(dedicatedAccounts));

        if(!accountNumber.isEmpty()) {
            String servedMsisdn = countryCode + accountNumber.trim();
            txRecord.put("SERVED_MSISDN", servedMsisdn);
        }

        if (adjustmentType.equalsIgnoreCase("2")) {
            String otherMsisdn = countryCode + externalData1.trim();
            txRecord.put("OTHER_MSISDN", otherMsisdn);
        }

        String ab = "0", ta = "0";
        if (!transactionAmount.trim().isEmpty()) ta = monetaryFormat.format(Double.parseDouble(transactionAmount.trim()));
        if (!accountBalance.trim().isEmpty()) ab = monetaryFormat.format(Double.parseDouble(accountBalance.trim()));
        txRecord.put("TRANSACTION_AMOUNT", ta);
        txRecord.put("ACCOUNT_BALANCE", ab);

        txRecord.put("ACCOUNT_FLAGS_BEFORE", accountFlagsBefore);
        txRecord.put("ACCOUNT_FLAGS_AFTER", accountFlagsAfter);
        txRecord.put("EXTERNAL_DATA1", externalData1);
        txRecord.put("EXTERNAL_DATA2", externalData2);
        return txRecord;
    }


    private Map<String, String> getSubscriptionTypeKey(String originHostName, String externalData1, String externalData2,
                                                       String transactionCode, String adjustmentType) {
        Map<String, String> subscriptionProperties = new HashMap<>();
        originHostName = originHostName.trim();
        externalData1 = externalData1.trim();
        externalData2 = externalData2.trim();
        transactionCode = transactionCode.trim();
        if (adjustmentType.equalsIgnoreCase("3")) {
            String subscriptionRecKey = "", serviceName = "";
            if ("USSDGWIAXF".equalsIgnoreCase(originHostName)) {
                if (externalData2.trim().toLowerCase().contains("faf")) {
                    subscriptionRecKey = "1";
                    serviceName = "FAF";
                } else if (externalData2.trim().toLowerCase().contains("sfd")) {
                    subscriptionRecKey = "2";
                    serviceName = "SFD";
                }
            }

            if (subscriptionRecKey.isEmpty()) {
                if ("QuotaBased".equalsIgnoreCase(externalData2)) {
                    subscriptionRecKey = "3";
                    serviceName = "QuotaBased";
                } else if (externalData2.toLowerCase().contains("flex") || externalData1.toLowerCase().contains("flex")) {
                    subscriptionRecKey = "4";
                    serviceName = "FLEX";
                } else if (externalData2.toLowerCase().contains("lte")) {
                    subscriptionRecKey = "5";
                    serviceName = "LTE Mixed Bundle";
                } else if (externalData2.toLowerCase().contains("data")) {
                    subscriptionRecKey = "6";
                    serviceName = "Data Package";
                } else if (externalData2.toLowerCase().contains("youth") || externalData1.toLowerCase().contains("youth")) {
                    subscriptionRecKey = "7";
                    serviceName = "Youth";
                } else if (externalData2.toLowerCase().contains("mass") || externalData1.toLowerCase().contains("mass")) {
                    subscriptionRecKey = "8";
                    serviceName = "MASS";
                }  else if (externalData2.toLowerCase().contains("cms") || externalData1.toLowerCase().contains("cms")) {
                    subscriptionRecKey = "9";
                    serviceName = "CMS";
                } else if (externalData2.toLowerCase().contains("donation")) {
                    subscriptionRecKey = "20";
                    serviceName = "Donation";
                } else {
                    subscriptionRecKey = "-98";
                    serviceName = "Others";
                }
            }


            subscriptionProperties.put("subscriptionRecKey", subscriptionRecKey);
            subscriptionProperties.put("serviceName", serviceName);
        }
        return subscriptionProperties;
    }

    private String getAdjustmentTypeKey(String originNodeType, String originHostName, String externalData2,
                                        String externalData1, String fileName) {
        String adjustmentTypeKey = "";
        originNodeType = originNodeType.trim();
        originHostName = originHostName.trim();
        externalData2 = externalData2.trim();
        externalData1 = externalData1.trim();
        if (!externalData2.isEmpty()) {
            if (externalData2.trim().toLowerCase().contains("faf") || externalData2.toLowerCase().contains("sfd") || externalData2.toLowerCase().contains("weekly")
                    || externalData2.trim().toLowerCase().contains("daily") || externalData2.toLowerCase().contains("monthly")
                    || externalData2.toLowerCase().contains("quotabased") || externalData2.equalsIgnoreCase("MIGRATION_REWARD")
                    || externalData2.toLowerCase().contains("youth") || externalData2.toLowerCase().contains("hac")
                    || externalData1.equalsIgnoreCase("CMS_Reward") || externalData1.toLowerCase().contains("flex")
                    || externalData1.toLowerCase().contains("bundle") || externalData1.toLowerCase().contains("gb")
                    || externalData1.toLowerCase().contains("cms") || externalData2.equalsIgnoreCase("Data_Roaming")
                    || externalData2.equalsIgnoreCase("HAC_SC") || externalData2.equalsIgnoreCase("STYL_EQ_3G")
                    || externalData2.toLowerCase().contains("bundle") || externalData2.toLowerCase().contains("hybrid")
                    || externalData2.toLowerCase().contains("mixed")|| externalData2.toLowerCase().contains("stylee")
                    || externalData2.toLowerCase().contains("gift") || externalData2.toLowerCase().contains("mass")
                    || externalData2.toLowerCase().contains("data") || externalData1.toLowerCase().contains("hac")
                    || externalData1.toLowerCase().contains("data") || externalData2.equalsIgnoreCase("VTS_PREPAID")
                    || externalData1.toLowerCase().contains("donation")) {
                adjustmentTypeKey = "3";
            }
        }
        if (adjustmentTypeKey.isEmpty()) {
            if ("EXT".equalsIgnoreCase(originNodeType) && "USSDGWIAXF".equalsIgnoreCase(originHostName)
                    && ("USSDGW-IAXF".equalsIgnoreCase(externalData2) || "USSDGW-IAXF ME2UFEE".equalsIgnoreCase(externalData2)
                    || "USSDGW-IAXF REVERSAL ME2UFEE".equalsIgnoreCase(externalData2)))
                adjustmentTypeKey = "2";
            if ("EXT".equalsIgnoreCase(originNodeType) && "OperationScript".equalsIgnoreCase(originHostName))
                adjustmentTypeKey = "1";
            if ("ADM".equalsIgnoreCase(originNodeType))
                adjustmentTypeKey = "1";
        }


        if (adjustmentTypeKey.isEmpty()) {
            logger.error("originNodeType - {}, originHostName - {}, externalData2 - {}, externalData1 - {}, " +
                    "fileName - {}", originNodeType, originHostName, externalData2, externalData1, fileName);
            adjustmentTypeKey = "-99";
        }
        return adjustmentTypeKey;
    }

    private LinkedHashMap<String, Object> getRefilledAccountInfo(List<LinkedHashMap<String, Object>> refillAccInfo, String type) {
        LinkedHashMap<String, Object> refillInfo = new LinkedHashMap<>();

        for (LinkedHashMap<String, Object> m : refillAccInfo) {
            Object temp = m.remove("usageAccumulator");
            if (temp != null) {
                List<LinkedHashMap<String, Object>> usageAccumulator = (List<LinkedHashMap<String, Object>>) temp;

                String uaID = dsv(usageAccumulator, "usageAccumulatorID");
                if (!uaID.trim().isEmpty()) refillInfo.put("usageAccumulatorID" + type, uaID);

                String uaVal = dsv(usageAccumulator, "usageAccumulatorValue");
                if (!uaVal.trim().isEmpty()) refillInfo.put("usageAccumulatorValue" + type, uaVal);

                String accStDate = dsv(usageAccumulator, "accumulatorStartDate");
                if (!accStDate.trim().isEmpty()) refillInfo.put("accumulatorStartDate" + type, accStDate);
            }
            temp = m.remove("offers");
            if (temp != null) {
                List<LinkedHashMap<String, Object>> offers = (List<LinkedHashMap<String, Object>>) temp;
                LinkedHashMap<String, Object> offersTx = getRefilledOffer(offers, type);
                refillInfo.putAll(offersTx);
            }
        }

        List<Object> dedicatedAccounts = refillAccInfo.stream()
                .filter(e -> e.containsKey("dedicatedAccounts"))
                .map(e -> e.get("dedicatedAccounts"))
                .collect(Collectors.toList());

        String[] tags = {"accountFlags", "accountBalance", "accumulatedRefillValue",
                "accumulatedRefillCounter", "accumulatedProgressionValue", "creditClearancePeriod",
                "promotionPlan", "permanentServiceClass", "temporaryServiceClass", "temporaryServiceClassExpiryDate",
                "refillOption", "serviceFeeExpiryDate", "serviceRemovalGracePeriod",
                "serviceOffering", "supervisionExpiryDate", "communityID1", "communityID2", "communityID3"};

//        for (String t : tags) {
//            String s = dsv(dedicatedAccounts, t);
//            if (!s.trim().isEmpty())
//                refillInfo.put(t + type, s);
//        }

        return refillInfo;
    }

    private LinkedHashMap<String, Object> getRefilledOffer(List<LinkedHashMap<String, Object>> offerInfo, String type) {
        LinkedHashMap<String, Object> ro = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> m : offerInfo) {
            Object temp = m.remove("attributeInformation");
            if (temp != null) {
                List<LinkedHashMap<String, Object>> attributeInformation = (List<LinkedHashMap<String, Object>>) temp;
                logger.debug("!!! attributeInformation !!!");
            }
        }
        String[] tags = {"offerIdentifier", "offerStartDate", "offerExpiryDate", "offerType",
                "offerProductIdentifier", "offerStartDateTime", "offerExpiryDateTime", "offerExpiryDateTime"};
        for (String t : tags) {
            String s = dsv(offerInfo, t);
            if (!s.trim().isEmpty())
                ro.put(t + type, s);
        }
        return ro;
    }


    private LinkedHashMap<String, Object> handleErrorRecords(LinkedHashMap<String, Object> record) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(record);
        String subscriberNumber = record.get("accountNumber").toString();

        String servedMsisdn = "";
        if(subscriberNumber != null){
            if(!subscriberNumber.isEmpty()){
                servedMsisdn += countryCode;
                servedMsisdn += subscriberNumber;
            }
        }
        txRecord.put("SERVED_MSISDN", servedMsisdn);
        return txRecord;
    }

    private void handleOfflinePromotionRecords(LinkedHashMap<String, Object> txRecord) {

    }


    private String handleDateObject(String rawDate) throws ParseException {
        String outDate = "";
        Date date;
        try {
            date = yyyyMMddHHmmss.get().parse(rawDate.trim());
            outDate = yyyyMMddFormatted.get().format(date);
        }catch (ParseException e){
            logger.error(e.getMessage(), e);
        }
        return outDate;
    }

    String dsv(List<LinkedHashMap<String, Object>> listOfMap, String key) {
        String c = listOfMap.stream()
                .filter(e -> e.containsKey(key))
                .map(e -> e.get(key).toString().trim())
                .reduce("", (x, y) -> x + y + "|");
        if (c != null && c.length() > 1) c = c.substring(0, c.length() - 1);
        return c;
    }

}

package com.gamma.skybase.build.server.etl.decoder.cbs_gprs;

import com.gamma.components.structure.IDatum;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class CBS_GPRSFileExecutor extends CBS_GPRSFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CBS_GPRSFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String [] headers = new String[]{"CDR_ID" , "CDR_SUB_ID" , "CDR_TYPE" , "SPLIT_CDR_REASON" , "CDR_BATCH_ID" , "SRC_REC_LINE_NO" , "SRC_CDR_ID" , "SRC_CDR_NO" , "STATUS" , "RE_RATING_TIMES" , "CREATE_DATE" , "START_DATE" , "END_DATE" , "CUST_LOCAL_START_DATE" , "CUST_LOCAL_END_DATE" , "STD_EVT_TYPE_ID" , "EVT_SOURCE_CATEGORY" , "OBJ_TYPE" , "OBJ_ID" , "OWNER_CUST_ID" , "DEFAULT_ACCT_ID" , "PRI_IDENTITY" , "BILL_CYCLE_ID" , "SERVICE_CATEGORY" , "USAGE_SERVICE_TYPE" , "SESSION_ID" , "RESULT_CODE" , "RESULT_REASON" , "BE_ID" , "HOT_SEQ" , "CP_ID" , "RECIPIENT_NUMBER" , "USAGE_MEASURE_ID" , "ACTUAL_USAGE" , "RATE_USAGE" , "SERVICE_UNIT_TYPE" , "USAGE_MEASURE_ID2" , "ACTUAL_USAGE2" , "RATE_USAGE2" , "SERVICE_UNIT_TYPE2" , "DEBIT_AMOUNT" , "UN_DEBIT_AMOUNT" , "DEBIT_FROM_PREPAID" , "DEBIT_FROM_ADVANCE_PREPAID" , "DEBIT_FROM_POSTPAID" , "DEBIT_FROM_ADVANCE_POSTPAID" , "DEBIT_FROM_CREDIT_POSTPAID" , "TOTAL_TAX" , "FREE_UNIT_AMOUNT_OF_TIMES" , "FREE_UNIT_AMOUNT_OF_DURATION" , "FREE_UNIT_AMOUNT_OF_FLUX" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "CURRENCY_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "FU_MEASURE_ID" , "OPER_TYPE" , "CallingPartyNumber" , "APN" , "URL" , "CallingPartyIMSI" , "AccessNetworkAddress" , "GGSNAddress" , "CallingRoamInfo" , "CallingCellID" , "TimeStampOfSGSN" , "TimeZoneOfSGSN" , "BearerCapability" , "ChargingTime" , "TotalFlux" , "UpFlux" , "DownFlux" , "ElapseDuration" , "IMEI" , "ServiceID" , "SPID" , "CategoryID" , "ContentID" , "QoS" , "BearerProtocolType" , "StartTime" , "StopTime" , "ChargingID" , "TransitionID" , " ServiceLevel" , " BrandID" , "MainOfferingID" , "ChargingPartyNumber" , "PayType" , "ChargingType" , "RoamState" , "CallingHomeCountryCode" , "CallingHomeAreaNumber" , "CallingHomeNetworkCode" , "CallingRoamCountryCode" , "CallingRoamAreaNumber" , "CallingRoamNetworkCode" , "ServiceType" , "CallingNetworkType" , "CallingVPNTopGroupNumber" , "CallingVPNGroupNumber" , "OnlineChargingFlag" , "StartTimeOfBillCycle" , "LastEffectOffering" , "DTDiscount" , "RatingGroup" , "HomeZoneID" , "SpecialZoneID" , "DataZoneID" , "MainBalanceInfo" , "ChgBalanceInfo" , "ChgFreeUnitInfo" , "UserState" , "GroupPayFlag" , "RATType" , "ChargePartyIndicator" , "RoamingZoneID" , "PrimaryOfferChgAmt" , "LostAmount" , "ADVANCE_PREPAID_BALANCE" , "CREDIT_POSTPAID_BALANCE" , "UNKNOWN"};

    @Override
    public void parseFile(String fileName) throws Exception {
        boolean jsonOutputRequired = dataSource.isRawJsonEnabled();
        String fn = null;
        Gson gson = null;
        if (jsonOutputRequired) {
            fn = new File(fileName).getName();
            gson = new GsonBuilder().setPrettyPrinting().create();
            jsonRecords = new LinkedList<>();
        }

        IEnrichment enrichment = null;
        try {
            Class<?> exec = Class.forName(dataSource.getTxExecClass());
            enrichment = (IEnrichment) exec.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }

        DelimitedFileDecoder decoder = null;
        try {
            long recCount = 0;

            decoder = new DelimitedFileDecoder(fileName, '|', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    record.put("fileName", metadata.decompFileName);
                    processRecord(record, enrichment);
                } catch (Exception e) {
                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> " , e.getMessage(), e);
                }
                recCount++;
                metadata.totalNoOfRecords = recCount;
            }
            if (metadata.noOfBadRecord > 0) metadata.status = "partial";
            if (metadata.noOfParsedRecord == 0 && recCount > 0) metadata.status = "failed";
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (decoder != null) decoder.close();
        }
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws Exception {
        if (enrichment != null) {
            MEnrichmentReq request = new MEnrichmentReq();
            request.setRequest(record);
            MEnrichmentResponse response = enrichment.transform(request);
            if (response.isResponseCode()) {
                LinkedHashMap<String, Object> data = response.getResponse();
                metadata.noOfParsedRecord++;
                if (data != null)
                    handleEvents("FCT_CBS_GPRS", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
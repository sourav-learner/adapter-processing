package com.gamma.skybase.build.server.etl.decoder.cbs_recharge;

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
public class CBS_RECHARGEFileExecutor extends CBS_RECHARGEFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CBS_RECHARGEFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String [] headers = new String[]{"RECHARGE_LOG_ID" , "RECHARGE_CODE" , "RECHARGE_AMT" , "ACCT_ID" , "SUB_ID" , "PRI_IDENTITY" , "THIRD_PARTY_NUMBER" , "CURRENCY_ID" , "ORIGINAL_AMT" , "CURRENCY_RATE" , "CONVERSION_AMT" , "RECHARGE_TRANS_ID" , "EXT_TRANS_TYPE" , "EXT_TRANS_ID" , "ACCESS_METHOD" , "BATCH_NO" , "OFFERING_ID" , "PAYMENT_TYPE" , "RECHARGE_TAX" , "RECHARGE_PENALTY" , "RECHARGE_TYPE" , "CHANNEL_ID" , "RECHARGE_REASON" , "RESULT_CODE" , "ERROR_TYPE" , "VALID_DAY_ADDED" , "DIAMETER_SESSIONID" , "OPER_ID" , "DEPT_ID" , "ENTRY_DATE" , "RECON_DATE" , "RECON_STATUS" , "REVERSAL_TRANS_ID" , "REVERSAL_REASON_CODE" , "REVERSAL_OPER_ID" , "REVERSAL_DEPT_ID" , "REVERSAL_DATE" , "STATUS" , "REMARK" , "BE_ID" , "BE_CODE" , "REGION_ID" , "REGION_CODE" , "CARD_SEQUENCE" , "CARD_PIN_NUMBER" , "CARD_BATCH_NO" , "CARD_STATUS" , "CARD_COS_ID" , "CARD_SP_ID" , "CARD_AMOUNT" , "CARD_VALIDITY" , "VOUCHER_ENCRYPT_NUMBER" , "CHECK_NO" , "CHECK_DATE" , "CREDIT_CARD_NO" , "CREDIT_CARD_NAME" , "CREDIT_CARD_TYPE_ID" , "CC_EXPIRY_DATE" , "CC_AUTHORIZATION_CODE" , "BANK_CODE" , "BANK_BRANCH_CODE" , "ACCT_NO" , "BANK_ACCT_NAME" , "LOAN_AMOUNT" , "LOAN_POUNDATE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID " , "FREE_UNIT_TYPE " , "CUR_AMOUNT " , "CHG_AMOUNT " , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID " , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID " , "ACCT_BALANCE_ID " , "BALANCE_TYPE " , "BONUS_AMOUNT " , "CURRENT_BALANCE" , "PRE_APPLY_TIME " , "PRE_EXPIRE_TIME " , "CUR_EXPIRE_TIME " , "CURRENCY_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE " , "FU_OWN_ID " , "FREE_UNIT_TYPE " , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "RechargeAreaCode" , "RechargeCellID" , "BrandID" , "MainOfferingID" , "PayType" , "StartTimeOfBillCycle" , "Account" , "MainBalanceInfo" , "ChgBalanceInfo" , "ChgFreeUnitInfo" , "UserState" , "OldUserState" , "CardValueAdded" , "ValidityAdded" , "TradeType" , "AgentName" , "AdditionalInfo" , "BankCode" , "SubIdentityType" , "Old_Validity" , "New_Validity" , "UNKNOWN"};

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
                    handleEvents("FCT_CBS_RECHARGE", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
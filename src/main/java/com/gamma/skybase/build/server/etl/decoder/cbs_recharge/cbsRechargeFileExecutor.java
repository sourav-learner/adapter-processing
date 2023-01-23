package com.gamma.skybase.build.server.etl.decoder.cbs_recharge;

import com.gamma.components.commons.constants.GammaConstants;
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
import java.io.FileWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("Duplicates")
public class cbsRechargeFileExecutor extends cbsRechargeFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(cbsRechargeFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    String [] headers = new String[]{"RECHARGE_LOG_ID" , "RECHARGE_CODE" , "RECHARGE_AMT" , "ACCT_ID" , "SUB_ID" , "PRI_IDENTITY" , "THIRD_PARTY_NUMBER" , "CURRENCY_ID" , "ORIGINAL_AMT" , "CURRENCY_RATE" , "CONVERSION_AMT" , "RECHARGE_TRANS_ID" , "EXT_TRANS_TYPE" , "EXT_TRANS_ID" , "ACCESS_METHOD" , "BATCH_NO" , "OFFERING_ID" , "PAYMENT_TYPE" , "RECHARGE_TAX" , "RECHARGE_PENALTY" , "RECHARGE_TYPE" , "CHANNEL_ID" , "RECHARGE_REASON" , "RESULT_CODE" , "ERROR_TYPE" , "VALID_DAY_ADDED" , "DIAMETER_SESSIONID" , "OPER_ID" , "DEPT_ID" , "ENTRY_DATE" , "RECON_DATE" , "RECON_STATUS" , "REVERSAL_TRANS_ID" , "REVERSAL_REASON_CODE" , "REVERSAL_OPER_ID" , "REVERSAL_DEPT_ID" , "REVERSAL_DATE" , "STATUS" , "REMARK" , "BE_ID" , "BE_CODE" , "REGION_ID" , "REGION_CODE" , "CARD_SEQUENCE" , "CARD_PIN_NUMBER" , "CARD_BATCH_NO" , "CARD_STATUS" , "CARD_COS_ID" , "CARD_SP_ID" , "CARD_AMOUNT" , "CARD_VALIDITY" , "VOUCHER_ENCRYPT_NUMBER" , "CHECK_NO" , "CHECK_DATE" , "CREDIT_CARD_NO" , "CREDIT_CARD_NAME" , "CREDIT_CARD_TYPE_ID" , "CC_EXPIRY_DATE" , "CC_AUTHORIZATION_CODE" , "BANK_CODE" , "BANK_BRANCH_CODE" , "ACCT_NO" , "BANK_ACCT_NAME" , "LOAN_AMOUNT" , "LOAN_POUNDATE" , "BC1_ACCT_ID" , "BC1_ACCT_BALANCE_ID" , "BC1_CUR_BALANCE" , "BC1_BALANCE_TYPE" , "BC1_CHG_BALANCE" , "BC1_PRE_APPLY_TIME" , "BC1_PRE_EXPIRE_TIME" , "BC1_CUR_EXPIRE_TIME" , "BC1_CURRENCYE_ID" , "BC1_OPER_TYPE" , "BC2_ACCT_ID" , "BC2_ACCT_BALANCE_ID" , "BC2_BALANCE_TYPE" , "BC2_CUR_BALANCE" , "BC2_CHG_BALANCE" , "BC2_PRE_APPLY_TIME" , "BC2_PRE_EXPIRE_TIME" , "BC2_CUR_EXPIRE_TIME" , "BC2_CURRENCYE_ID" , "BC2__OPER_TYPE" , "BC3_ACCT_ID" , "BC3_ACCT_BALANCE_ID" , "BC3_BALANCE_TYPE" , "BC3_CUR_BALANCE" , "BC3_CHG_BALANCE" , "BC3_PRE_APPLY_TIME" , "BC3_PRE_EXPIRE_TIME" , "BC3_CUR_EXPIRE_TIME" , "BC3_CURRENCYE_ID" , "BC3_OPER_TYPE" , "BC4_ACCT_ID" , "BC4_ACCT_BALANCE_ID" , "BC4_BALANCE_TYPE" , "BC4_CUR_BALANCE" , "BC4_CHG_BALANCE" , "BC4_PRE_APPLY_TIME" , "BC4_PRE_EXPIRE_TIME" , "BC4_CUR_EXPIRE_TIME" , "BC4_CURRENCYE_ID" , "BC4_OPER_TYPE" , "BC5_ACCT_ID" , "BC5_ACCT_BALANCE_ID" , "BC5_BALANCE_TYPE" , "BC5_CUR_BALANCE" , "BC5_CHG_BALANCE" , "BC5_PRE_APPLY_TIME" , "BC5_PRE_EXPIRE_TIME" , "BC5_CUR_EXPIRE_TIME" , "BC5_CURRENCYE_ID" , "BC5_OPER_TYPE" , "BC6_ACCT_ID" , "BC6_ACCT_BALANCE_ID" , "BC6_BALANCE_TYPE" , "BC6_CUR_BALANCE" , "BC6_CHG_BALANCE" , "BC6_PRE_APPLY_TIME" , "BC6_PRE_EXPIRE_TIME" , "BC6_CUR_EXPIRE_TIME" , "BC6_CURRENCYE_ID" , "BC6_OPER_TYPE" , "BC7_ACCT_ID" , "BC7_ACCT_BALANCE_ID" , "BC7_BALANCE_TYPE" , "BC7_CUR_BALANCE" , "BC7_CHG_BALANCE" , "BC7_PRE_APPLY_TIME" , "BC7_PRE_EXPIRE_TIME" , "BC7_CUR_EXPIRE_TIME" , "BC7_CURRENCYE_ID" , "BC7_OPER_TYPE" , "BC8_ACCT_ID" , "BC8_ACCT_BALANCE_ID" , "BC8_BALANCE_TYPE" , "BC8_CUR_BALANCE" , "BC8_CHG_BALANCE" , "BC8_PRE_APPLY_TIME" , "BC8_PRE_EXPIRE_TIME" , "BC8_CUR_EXPIRE_TIME" , "BC8_CURRENCYE_ID" , "BC8_OPER_TYPE" , "BC9_ACCT_ID" , "BC9_ACCT_BALANCE_ID" , "BC9_BALANCE_TYPE" , "BC9_CUR_BALANCE" , "BC9_CHG_BALANCE" , "BC9_PRE_APPLY_TIME" , "BC9_PRE_EXPIRE_TIME" , "BC9_CUR_EXPIRE_TIME" , "BC9_CURRENCYE_ID" , "BC9_OPER_TYPE" , "BC10_ACCT_ID" , "BC10_ACCT_BALANCE_ID" , "BC10_BALANCE_TYPE" , "BC10_CUR_BALANCE" , "BC10_CHG_BALANCE" , "BC10_PRE_APPLY_TIME" , "BC10_PRE_EXPIRE_TIME" , "BC10_CUR_EXPIRE_TIME" , "BC10_CURRENCYE_ID" , "BC10_OPER_TYPE" , "FC1_FU_OWN_TYPE" , "FC1_FU_OWN_ID" , "FC1_FREE_UNIT_ID" , "FC1_FREE_UNIT_TYPE" , "FC1_CUR_AMOUNT" , "FC1_CHG_AMOUNT" , "FC1_PRE_APPLY_TIME" , "FC1_PRE_EXPIRE_TIME" , "FC1_CUR_EXPIRE_TIME" , "FC1_FU_MEASURE_ID" , "FC1_OPER_TYPE" , "FC2_FU_OWN_TYPE" , "FC2_FU_OWN_ID" , "FC2_FREE_UNIT_ID" , "FC2_FREE_UNIT_TYPE" , "FC2_CUR_AMOUNT" , "FC2_CHG_AMOUNT" , "FC2_PRE_APPLY_TIME" , "FC2_PRE_EXPIRE_TIME" , "FC2_CUR_EXPIRE_TIME" , "FC2_FU_MEASURE_ID" , "FC2_OPER_TYPE" , "FC3_FU_OWN_TYPE" , "FC3_FU_OWN_ID" , "FC3_FREE_UNIT_ID" , "FC3_FREE_UNIT_TYPE" , "FC3_CUR_AMOUNT" , "FC3_CHG_AMOUNT" , "FC3_PRE_APPLY_TIME" , "FC3_PRE_EXPIRE_TIME" , "FC3_CUR_EXPIRE_TIME" , "FC3_FU_MEASURE_ID" , "FC3_OPER_TYPE" , "FC4_FU_OWN_TYPE" , "FC4_FU_OWN_ID" , "FC4_FREE_UNIT_ID" , "FC4_FREE_UNIT_TYPE" , "FC4_CUR_AMOUNT" , "FC4_CHG_AMOUNT" , "FC4_PRE_APPLY_TIME" , "FC4_PRE_EXPIRE_TIME" , "FC4_CUR_EXPIRE_TIME" , "FC4_FU_MEASURE_ID" , "FC4_OPER_TYPE" , "F5_FU_OWN_TYPE" , "FC5_FU_OWN_ID" , "FC5_FREE_UNIT_ID" , "FC5_FREE_UNIT_TYPE" , "FC5_CUR_AMOUNT" , "FC5_CHG_AMOUNT" , "FC5_PRE_APPLY_TIME" , "FC5_PRE_EXPIRE_TIME" , "FC5_CUR_EXPIRE_TIME" , "FC5_FU_MEASURE_ID" , "FC5_OPER_TYPE" , "FC6_FU_OWN_TYPE" , "FC6_FU_OWN_ID" , "FC6_FREE_UNIT_ID" , "FC6_FREE_UNIT_TYPE" , "FC6_CUR_AMOUNT" , "FC6_CHG_AMOUNT" , "FC6_PRE_APPLY_TIME" , "FC6_PRE_EXPIRE_TIME" , "FC6_CUR_EXPIRE_TIME" , "FC6_FU_MEASURE_ID" , "FC6_OPER_TYPE" , "FC7_FU_OWN_TYPE" , "FC7_FU_OWN_ID" , "FC7_FREE_UNIT_ID" , "FC7_FREE_UNIT_TYPE" , "FC7_CUR_AMOUNT" , "FC7_CHG_AMOUNT" , "FC7_PRE_APPLY_TIME" , "FC7_PRE_EXPIRE_TIME" , "FC7_CUR_EXPIRE_TIME" , "FC7_FU_MEASURE_ID" , "FC7_OPER_TYPE" , "FC8_FU_OWN_TYPE" , "FC8_FU_OWN_ID" , "FC8_FREE_UNIT_ID" , "FC8_FREE_UNIT_TYPE" , "FC8_CUR_AMOUNT" , "FC8_CHG_AMOUNT" , "FC8_PRE_APPLY_TIME" , "FC8_PRE_EXPIRE_TIME" , "FC8_CUR_EXPIRE_TIME" , "FC8_FU_MEASURE_ID" , "FC8_OPER_TYPE" , "FC9_FU_OWN_TYPE" , "FC9_FU_OWN_ID" , "FC9_FREE_UNIT_ID" , "FC9_FREE_UNIT_TYPE" , "FC9_CUR_AMOUNT" , "FC9_CHG_AMOUNT" , "FC9_PRE_APPLY_TIME" , "FC9_PRE_EXPIRE_TIME" , "FC9_CUR_EXPIRE_TIME" , "FC9_FU_MEASURE_ID" , "FC9_OPER_TYPE" , "FC10_FU_OWN_TYPE" , "FC10_FU_OWN_ID" , "FC10_FREE_UNIT_ID" , "FC10_FREE_UNIT_TYPE" , "FC10_CUR_AMOUNT" , "FC10_CHG_AMOUNT" , "FC10_PRE_APPLY_TIME" , "FC10_PRE_EXPIRE_TIME" , "FC10_CUR_EXPIRE_TIME" , "FC10_FU_MEASURE_ID" , "FC10_OPER_TYPE" , "BD1_ACCT_ID" , "BD1_ACCT_BALANCE_ID" , "BD1_BALANCE_TYPE" , "BD1_BONUS_AMOUNT" , "BD1_CURRENT_BALANCE" , "BD1_PRE_APPLY_TIME" , "BD1_PRE_EXPIRE_TIME" , "BD1_CUR_EXPIRE_TIME" , "BD1_CURRENCY_ID" , "BD1_OPER_TYPE" , "BD2_ACCT_ID" , "BD2_ACCT_BALANCE_ID" , "BD2_BALANCE_TYPE" , "BD2_BONUS_AMOUNT" , "BD2_CURRENT_BALANCE" , "BD2_PRE_APPLY_TIME" , "BD2_PRE_EXPIRE_TIME" , "BD2_CUR_EXPIRE_TIME" , "BD2_CURRENCY_ID" , "BD2_OPER_TYPE" , "BD3_ACCT_ID" , "BD3_ACCT_BALANCE_ID" , "BD3_BALANCE_TYPE" , "BD3_BONUS_AMOUNT" , "BD3_CURRENT_BALANCE" , "BD3_PRE_APPLY_TIME" , "BD3_PRE_EXPIRE_TIME" , "BD3_CUR_EXPIRE_TIME" , "BD3_CURRENCY_ID" , "BD3_OPER_TYPE" , "BD4_ACCT_ID" , "BD4_ACCT_BALANCE_ID" , "BD4_BALANCE_TYPE" , "BD4_BONUS_AMOUNT" , "BD4_CURRENT_BALANCE" , "BD4_PRE_APPLY_TIME" , "BD4_PRE_EXPIRE_TIME" , "BD4_CUR_EXPIRE_TIME" , "BD4_CURRENCY_ID" , "BD4_OPER_TYPE" , "BD5_ACCT_ID" , "BD5_ACCT_BALANCE_ID" , "BD5_BALANCE_TYPE" , "BD5_BONUS_AMOUNT" , "BD5_CURRENT_BALANCE" , "BD5_PRE_APPLY_TIME" , "BD5_PRE_EXPIRE_TIME" , "BD5_CUR_EXPIRE_TIME" , "BD5_CURRENCY_ID" , "BD5_OPER_TYPE" , "BD6_ACCT_ID" , "BD6_ACCT_BALANCE_ID" , "BD6_BALANCE_TYPE" , "BD6_BONUS_AMOUNT" , "BD6_CURRENT_BALANCE" , "BD6_PRE_APPLY_TIME" , "BD6_PRE_EXPIRE_TIME" , "BD6_CUR_EXPIRE_TIME" , "BD6_CURRENCY_ID" , "BD6_OPER_TYPE" , "BD7_ACCT_ID" , "BD7_ACCT_BALANCE_ID" , "BD7_BALANCE_TYPE" , "BD7_BONUS_AMOUNT" , "BD7_CURRENT_BALANCE" , "BD7_PRE_APPLY_TIME" , "BD7_PRE_EXPIRE_TIME" , "BD7_CUR_EXPIRE_TIME" , "BD7_CURRENCY_ID" , "BD7_OPER_TYPE" , "BD8_ACCT_ID" , "BD8_ACCT_BALANCE_ID" , "BD8_BALANCE_TYPE" , "BD8_BONUS_AMOUNT" , "BD8_CURRENT_BALANCE" , "BD8_PRE_APPLY_TIME" , "BD8_PRE_EXPIRE_TIME" , "BD8_CUR_EXPIRE_TIME" , "BD8_CURRENCY_ID" , "BD8_OPER_TYPE" , "BD9_ACCT_ID" , "BD9_ACCT_BALANCE_ID" , "BD9_BALANCE_TYPE" , "BD9_BONUS_AMOUNT" , "BD9_CURRENT_BALANCE" , "BD9_PRE_APPLY_TIME" , "BD9_PRE_EXPIRE_TIME" , "BD9_CUR_EXPIRE_TIME" , "BD9_CURRENCY_ID" , "BD9_OPER_TYPE" , "BD10_ACCT_ID" , "BD10_ACCT_BALANCE_ID" , "BD10_BALANCE_TYPE" , "BD10_BONUS_AMOUNT" , "BD10_CURRENT_BALANCE" , "BD10_PRE_APPLY_TIME" , "BD10_PRE_EXPIRE_TIME" , "BD10_CUR_EXPIRE_TIME" , "BD10_CURRENCY_ID" , "BD10_OPER_TYPE" , "FR1_FU_OWN_TYPE" , "FR1_FU_OWN_ID" , "FR1_FREE_UNIT_TYPE" , "FR1_FREE_UNIT_ID" , "FR1_BONUS_AMOUNT" , "FR1_CURRENT_AMOUNT" , "FR1_PRE_APPLY_TIME" , "FR1_PRE_EXPIRE_TIME" , "FR1_CUR_EXPIRE_TIME" , "FR1_FU_MEASURE_ID" , "FR1_OPER_TYPE" , "FR2_FU_OWN_TYPE" , "FR2_FU_OWN_ID" , "FR2_FREE_UNIT_TYPE" , "FR2_FREE_UNIT_ID" , "FR2_BONUS_AMOUNT" , "FR2_CURRENT_AMOUNT" , "FR2_PRE_APPLY_TIME" , "FR2_PRE_EXPIRE_TIME" , "FR2_CUR_EXPIRE_TIME" , "FR2_FU_MEASURE_ID" , "FR2_OPER_TYPE" , "FR3_FU_OWN_TYPE" , "FR3_FU_OWN_ID" , "FR3_FREE_UNIT_TYPE" , "FR3_FREE_UNIT_ID" , "FR3_BONUS_AMOUNT" , "FR3_CURRENT_AMOUNT" , "FR3_PRE_APPLY_TIME" , "FR3_PRE_EXPIRE_TIME" , "FR3_CUR_EXPIRE_TIME" , "FR3_FU_MEASURE_ID" , "FR3_OPER_TYPE" , "FR4_FU_OWN_TYPE" , "FR4_FU_OWN_ID" , "FR4_FREE_UNIT_TYPE" , "FR4_FREE_UNIT_ID" , "FR4_BONUS_AMOUNT" , "FR4_CURRENT_AMOUNT" , "FR4_PRE_APPLY_TIME" , "FR4_PRE_EXPIRE_TIME" , "FR4_CUR_EXPIRE_TIME" , "FR4_FU_MEASURE_ID" , "FR4_OPER_TYPE" , "FR5_FU_OWN_TYPE" , "FR5_FU_OWN_ID" , "FR5_FREE_UNIT_TYPE" , "FR5_FREE_UNIT_ID" , "FR5_BONUS_AMOUNT" , "FR5_CURRENT_AMOUNT" , "FR5_PRE_APPLY_TIME" , "FR5_PRE_EXPIRE_TIME" , "FR5_CUR_EXPIRE_TIME" , "FR5_FU_MEASURE_ID" , "FR5_OPER_TYPE" , "FR6_FU_OWN_TYPE" , "FR6_FU_OWN_ID" , "FR6_FREE_UNIT_TYPE" , "FR6_FREE_UNIT_ID" , "FR6_BONUS_AMOUNT" , "FR6_CURRENT_AMOUNT" , "FR6_PRE_APPLY_TIME" , "FR6_PRE_EXPIRE_TIME" , "FR6_CUR_EXPIRE_TIME" , "FR6_FU_MEASURE_ID" , "FR6_OPER_TYPE" , "FR7_FU_OWN_TYPE" , "FR7_FU_OWN_ID" , "FR7_FREE_UNIT_TYPE" , "FR7_FREE_UNIT_ID" , "FR7_BONUS_AMOUNT" , "FR7_CURRENT_AMOUNT" , "FR7_PRE_APPLY_TIME" , "FR7_PRE_EXPIRE_TIME" , "FR7_CUR_EXPIRE_TIME" , "FR7_FU_MEASURE_ID" , "FR7_OPER_TYPE" , "FR8_FU_OWN_TYPE" , "FR8_FU_OWN_ID" , "FR8_FREE_UNIT_TYPE" , "FR8_FREE_UNIT_ID" , "FR8_BONUS_AMOUNT" , "FR8_CURRENT_AMOUNT" , "FR8_PRE_APPLY_TIME" , "FR8_PRE_EXPIRE_TIME" , "FR8_CUR_EXPIRE_TIME" , "FR8_FU_MEASURE_ID" , "FR8_OPER_TYPE" , "FR9_FU_OWN_TYPE" , "FR9_FU_OWN_ID" , "FR9_FREE_UNIT_TYPE" , "FR9_FREE_UNIT_ID" , "FR9_BONUS_AMOUNT" , "FR9_CURRENT_AMOUNT" , "FR9_PRE_APPLY_TIME" , "FR9_PRE_EXPIRE_TIME" , "FR9_CUR_EXPIRE_TIME" , "FR9_FU_MEASURE_ID" , "FR9_OPER_TYPE" , "FR10_FU_OWN_TYPE" , "FR10_FU_OWN_ID" , "FR10_FREE_UNIT_TYPE" , "FR10_FREE_UNIT_ID" , "FR10_BONUS_AMOUNT" , "FR10_CURRENT_AMOUNT" , "FR10_PRE_APPLY_TIME" , "FR10_PRE_EXPIRE_TIME" , "FR10_CUR_EXPIRE_TIME" , "FR10_FU_MEASURE_ID" , "FR10_OPER_TYPE" , "RECHARGE_AREA_CODE" , "RECHARGE_CELL_ID" , "BRAND_ID" , "MAIN_OFFERING_ID" , "PayType" , "START_TIME_OF_BILL_CYCLE" , "ACCOUNT" , "MAIN_BALANCE_INFO" , "CHG_BALANCE_INFO" , "CHG_FREE_UNIT_INFO" , "USER_STATE" , "OLD_USER_STATE" , "CARD_VALUE_ADDED" , "VALIDITY_ADDED" , "TRADE_TYPE" , "AGENT_NAME" , "ADDITIONAL_INFO" , "BANK_CODE" , "SUB_IDENTITY_TYPE" , "OLD_VALIDITY" , "NEW_VALIDITY" , "UNKNOWN"};

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
                    if (jsonOutputRequired) jsonRecords.add(record);

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

            if (jsonOutputRequired) {
                FileWriter writer = new FileWriter("out" + GammaConstants.PATH_SEP + fn + ".json");
                writer.write(gson.toJson(jsonRecords));
                writer.flush();
            }
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
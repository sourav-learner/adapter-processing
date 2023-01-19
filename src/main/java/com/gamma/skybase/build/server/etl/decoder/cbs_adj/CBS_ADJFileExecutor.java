package com.gamma.skybase.build.server.etl.decoder.cbs_adj;

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
public class CBS_ADJFileExecutor extends CBS_ADJFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CBS_ADJFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;
    String [] headers = new String[]{"ADJUST_LOG_ID" , "ACCT_ID" , "CUST_ID" , "SUB_ID" , "PRI_IDENTITY" , "PAY_TYPE" , "BATCH_NO" , "CHANNEL_ID" , "REASON_CODE" , "RESULT_CODE" , "ERROR_TYPE" , "ACCT_BALANCE_ID" , "ADJUST_AMT" , "ADJUST_TRANS_ID" , "EXT_TRANS_TYPE" , "EXT_TRANS_ID" , "ACCESS_METHOD" , "REVERSAL_TRANS_ID" , "REVERSAL_REASON_CODE" , "REVERSAL_DATE" , "STATUS" , "ENTRY_DATE" , "OPER_ID" , "DEPT_ID" , "REMARK" , "BE_ID" ,"BE_CODE" , "REGION_ID" , "REGION_CODE" , "DEBIT_AMOUNT" , "UN_DEBIT_AMOUNT" , "DEBIT_FROM_PREPAID" , "DEBIT_FROM_ADVANCE_PREPAID" , "DEBIT_FROM_POSTPAID" , "DEBIT_FROM_ADVANCE_POSTPAID" , "DEBIT_FROM_CREDIT_POSTPAID" , "TOTAL_TAX" , "FREE_UNIT_AMOUNT_OF_TIMES" , "FREE_UNIT_AMOUNT_OF_DURATION" , "FREE_UNIT_AMOUNT_OF_FLUX" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "CUR_BALANCE" , "CHG_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCYE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_ID" , "FREE_UNIT_TYPE" , "CUR_AMOUNT" , "CHG_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "ACCT_ID" , "ACCT_BALANCE_ID" , "BALANCE_TYPE" , "BONUS_AMOUNT" , "CURRENT_BALANCE" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "CURRENCY_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "FU_OWN_TYPE" , "FU_OWN_ID" , "FREE_UNIT_TYPE" , "FREE_UNIT_ID" , "BONUS_AMOUNT" , "CURRENT_AMOUNT" , "PRE_APPLY_TIME" , "PRE_EXPIRE_TIME" , "CUR_EXPIRE_TIME" , "FU_MEASURE_ID" , "OPER_TYPE" , "BrandID" , "MainOfferingID" , "PayType" , "StartTimeOfBillCycle" , "Account" , "MainBalanceInfo" , "ChgBalanceInfo" , "ChgFreeUnitInfo" , "UserState" , "oldUserState" , "SPID" , "AdditionalInfo" , "Merchant" , "Service" , "UNKNOWN"};

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
                    handleEvents("FCT_CBS_ADJ", data);
            }
        }
    }

    @Override
    public void doCustomization(String eventName, Map<String, IDatum> record) throws Exception {

    }
}
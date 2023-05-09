package com.gamma.skybase.build.server.etl.decoder.cbs_adj;

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
import java.util.*;

@SuppressWarnings("Duplicates")
public class CbsAdjFileExecutor extends CbsAdjFileProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CbsAdjFileExecutor.class);
    private List<LinkedHashMap<String, Object>> jsonRecords;

    String[] headers = new String[]{"ADJUST_LOG_ID", "ACCT_ID", "CUST_ID", "SUB_ID", "PRI_IDENTITY", "PAY_TYPE", "BATCH_NO", "CHANNEL_ID", "REASON_CODE", "RESULT_CODE", "ERROR_TYPE", "ACCT_BALANCE_ID", "ADJUST_AMT", "ADJUST_TRANS_ID", "EXT_TRANS_TYPE", "EXT_TRANS_ID", "ACCESS_METHOD", "REVERSAL_TRANS_ID", "REVERSAL_REASON_CODE", "REVERSAL_DATE", "STATUS", "ENTRY_DATE", "OPER_ID", "DEPT_ID", "REMARK", "BE_ID", "BE_CODE", "REGION_ID", "REGION_CODE", "TOTAL_FEE", "TOTAL_FREE_UNIT", "CHARGE_SERVICE_INFO", "CHG_BALANCE", "CHG_FREE_UNIT", "BONUS_FUND", "BONUS_FREE_UNIT", "BONUS_OFFERING", "INSTALMENT", "CHG_ACCUMULATE", "CHG_SPENDING_LIMIT", "CHG_PAY_LIMIT", "CHG_CREDIT_LIMIT", "CHG_BALANCE_LIMIT", "CHG_FU_LIMIT", "SUB_LIFECYCLE", "ADDITIONAL_INFO", "LOAN_INFO", "TAX_INFO"};

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
            long recCount = 1;

            decoder = new DelimitedFileDecoder(fileName, '|', headers, 0);
            while (decoder.hasNext()) {
                try {
                    LinkedHashMap<String, Object> record = decoder.next();
                    if (jsonOutputRequired) jsonRecords.add(record);

                    String tot = record.get("TOTAL_FREE_UNIT").toString();
                    String a = trim(tot);

//                    record.put("DEBIT_AMOUNT", String.valueOf(tot1[0]));
//                    record.put("UN_DEBIT_AMOUNT", String.valueOf(tot1[1]));
//                    record.put("DEBIT_FROM_PREPAID", String.valueOf(tot1[2]));
//                    record.put("DEBIT_FROM_ADVANCE_PREPAID", String.valueOf(tot1[3]));
//                    record.put("DEBIT_FROM_POSTPAID", String.valueOf(tot1[4]));
//                    record.put("DEBIT_FROM_ADVANCE_POSTPAID", String.valueOf(tot1[5]));
//                    record.put("DEBIT_FROM_CREDIT_POSTPAID", String.valueOf(tot1[6]));
//                    record.put("TOTAL_TAX", String.valueOf(tot1[7]));
//                    record.put("CURRENCY_ID", String.valueOf(tot1[8]));

                    ArrayList<String> totalFree = new ArrayList<>();
                    totalFree.add("DEBIT_AMOUNT");
                    totalFree.add("UN_DEBIT_AMOUNT");
                    totalFree.add("DEBIT_FROM_PREPAID");
                    totalFree.add("DEBIT_FROM_ADVANCE_PREPAID");
                    totalFree.add("DEBIT_FROM_POSTPAID");
                    totalFree.add("DEBIT_FROM_ADVANCE_POSTPAID");
                    totalFree.add("DEBIT_FROM_CREDIT_POSTPAID");
                    totalFree.add("TOTAL_TAX");
                    totalFree.add("CURRENCY_ID");

                    String[] total = a.split(",");

                    int totalFreeUnit = total.length;

                    for(int i = 0 ; i < totalFreeUnit; i++){
                        record.put(totalFree.get(i),String.valueOf(total[i]));
                    }

                    String chgbalance = record.get("CHG_BALANCE").toString();
                    String cb = trim(chgbalance);

                    ArrayList<String> chgBalance = new ArrayList<>();
                    chgBalance.add("ACCT_ID1");
                    chgBalance.add("ACCT_BALANCE_ID1");
                    chgBalance.add("BALANCE_TYPE1");
                    chgBalance.add("CUR_BALANCE1");
                    chgBalance.add("CHG_BALANCE1");
                    chgBalance.add("PRE_APPLY_TIME");
                    chgBalance.add("PRE_EXPIRE_TIME");
                    chgBalance.add("CUR_EXPIRE_TIME");
                    chgBalance.add("CURRENCY_ID");
                    chgBalance.add("OPER_TYPE");
                    chgBalance.add("DAILY_BILL_DATE");
                    chgBalance.add("CHG_FREE_UNIT");
                    String[] chrg_balance = cb.split(",");

                    int chBalance = chrg_balance.length;

                    for(int i = 0 ; i < chBalance; i++){
                        record.put(chgBalance.get(i),String.valueOf(chrg_balance[i]));
                    }

                    String subLifecycle = record.get("SUB_LIFECYCLE").toString();
                    String subLifeC = trim(subLifecycle);

                    ArrayList<String> subLife = new ArrayList<>();
                    subLife.add("OLD_STATUS");
                    subLife.add("CURRENT_STATUS");
                    subLife.add("OLD_DETAILED_STATUS");
                    subLife.add("CURRENT_DETAILED_STATUS");
                    subLife.add("ACTIVE_LIMIT_TIME");
                    subLife.add("OLD_ACTIVE_TIME");
                    subLife.add("CUR_ACTIVE_TIME");
                    subLife.add("OLD_S1_EXP_DATE");
                    subLife.add("CUR_S1_EXP_DATE");
                    subLife.add("OLD_S2_EXP_DATE");
                    subLife.add("CUR_S2_EXP_DATE");
                    subLife.add("OLD_S3_EXP_DATE");
                    subLife.add("CUR_S3_EXP_DATE");
                    subLife.add("OLD_S4_EXP_DATE");
                    subLife.add("CUR_S4_EXP_DATE");
                    subLife.add("OLD_S5_EXP_DATE");
                    subLife.add("CUR_S5_EXP_DATE");
                    subLife.add("OLD_S6_EXP_DATE");
                    subLife.add("CUR_S6_EXP_DATE");
                    subLife.add("OLD_S7_EXP_DATE");
                    subLife.add("CUR_S7_EXP_DATE");
                    subLife.add("OLD_S8_EXP_DATE");
                    subLife.add("CUR_S8_EXP_DATE");
                    subLife.add("OLD_S9_EXP_DATE");
                    subLife.add("CUR_S9_EXP_DATE");

                    String[] subLifeCycle = subLifeC.split(",");
                    int lastInt = subLifeCycle.length;

                    for(int i = 0 ; i < lastInt; i++){
                        record.put(subLife.get(i),String.valueOf(subLifeCycle[i]));
                    }

                    String additional_info = record.get("ADDITIONAL_INFO").toString();
                    String[] additionalInfo = additional_info.split(",");

                    ArrayList<String> additional = new ArrayList<>();
                    additional.add("BrandID");
                    additional.add("MainOfferingID");
                    additional.add("PayType");
                    additional.add("StartTimeOfBillCycle");
                    additional.add("AdditionalInfo");
                    additional.add("SPCode");
                    additional.add("Merchant");
                    additional.add("Service");
                    additional.add("Reserve");
                    additional.add("TransactionCode");
                    additional.add("BILL_CYCLE_ID");

                    int aI= additionalInfo.length;

                    for(int i = 0 ; i < aI; i++) {
                        record.put(additional.get(i), String.valueOf(additionalInfo[i]));
                    }

                    String loan_info = record.get("LOAN_INFO").toString();
                    String loanIn = trim(loan_info);

                    ArrayList<String> loan = new ArrayList<>();
                    loan.add("LOAN_AMOUNT");
                    loan.add("LOAN_POUNDAGE");
                    loan.add("LOAN_PENALTY");

                    String[] loans = loanIn.split(",");

                    int loanInfo = loans.length;

                    for(int i = 0 ; i < loanInfo; i++){
                        record.put(loan.get(i),String.valueOf(loans[i]));
                    }

                    record.put("fileName", metadata.decompFileName);
                    record.put("_SEQUENCE_NUMBER",recCount);
                    processRecord(record, enrichment);

                } catch (Exception e) {
                    e.printStackTrace();
                    metadata.comments = "Parsing issues";
                    metadata.noOfBadRecord++;
                    logger.error(dataSource.getAdapterName() + " -> ", e.getMessage(), e);
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

    public static String trim(String value) {
        int len = value.length();
        int st = 0;
        while (st < len && value.charAt(st) == '{')
            st++;

        int end = 1;
        while ( value.charAt(len - end) == '}')
            end++;

        return value.substring(st, len - end+1);
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

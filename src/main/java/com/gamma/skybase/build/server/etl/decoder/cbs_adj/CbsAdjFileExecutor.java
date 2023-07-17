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

    String[] headers = new String[]{"ADJUST_LOG_ID", "ACCT_ID", "CUST_ID", "SUB_ID", "PRI_IDENTITY", "PAY_TYPE", "BATCH_NO", "CHANNEL_ID", "REASON_CODE", "RESULT_CODE", "ERROR_TYPE", "ACCT_BALANCE_ID", "ADJUST_AMT", "ADJUST_TRANS_ID", "EXT_TRANS_TYPE", "EXT_TRANS_ID", "ACCESS_METHOD", "REVERSAL_TRANS_ID", "REVERSAL_REASON_CODE", "REVERSAL_DATE", "STATUS", "ENTRY_DATE", "OPER_ID", "DEPT_ID", "REMARK", "BE_ID", "BE_CODE", "REGION_ID", "REGION_CODE", "CHARGE_SERVICE_INFO", "TOTAL_FEE", "TOTAL_FREE_UNIT", "CHG_BALANCE", "CHG_FREE_UNIT", "BONUS_FUND", "BONUS_FREE_UNIT", "BONUS_OFFERING", "INSTALMENT", "CHG_ACCUMULATE", "CHG_SPENDING_LIMIT", "CHG_PAY_LIMIT", "CHG_CREDIT_LIMIT", "CHG_BALANCE_LIMIT", "CHG_FU_LIMIT", "SUB_LIFECYCLE", "ADDITIONAL_INFO", "LOAN_INFO", "TAX_INFO"};

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

                    String[] tot = new String[]{record.get("TOTAL_FEE").toString()};
                    ArrayList<String> totalFee = new ArrayList<>();
                    totalFee.add("DEBIT_AMOUNT");
                    totalFee.add("UN_DEBIT_AMOUNT");
                    totalFee.add("DEBIT_FROM_PREPAID");
                    totalFee.add("DEBIT_FROM_ADVANCE_PREPAID");
                    totalFee.add("DEBIT_FROM_POSTPAID");
                    totalFee.add("DEBIT_FROM_ADVANCE_POSTPAID");
                    totalFee.add("DEBIT_FROM_CREDIT_POSTPAID");
                    totalFee.add("TOTAL_TAX");
                    totalFee.add("CURRENCY_ID");

                    int row = tot.length;
                    for (int i = 0; i < row; i++) {
                        if (tot[i].equalsIgnoreCase("")) {
                            for(int j=0;j<totalFee.size();j++){
                                record.put(totalFee.get(j),tot[i]);
                            }
                        }
                        else {
                            String tmptotals = tot[i];
                            tmptotals =  tmptotals.replace("[{","");
                            tmptotals = tmptotals.replace("}]","");
                            String a = trim(tmptotals);
                            String[] totals = a.split(",");
                            int totalFre = totals.length;

                            for (int j = 0; j < totalFre; j++) {
                                record.put(totalFee.get(i), String.valueOf(totals[i]));
                            }
                        }
                    }

                    String[] total = new String[]{record.get("TOTAL_FREE_UNIT").toString()};
                    ArrayList<String> totalFreeUnit = new ArrayList<>();
                    totalFreeUnit.add("FREE_UNIT_AMOUNT_OF_TIMES");
                    totalFreeUnit.add("FREE_UNIT_AMOUNT_OF_DURATION");
                    totalFreeUnit.add("FREE_UNIT_AMOUNT_OF_FLUX");
                    totalFreeUnit.add("FREE_UNIT_AMOUNT_OF_SPEC");
                    int rowSize = total.length;
                    for (int i = 0; i < rowSize; i++) {
                        if (total[i].equalsIgnoreCase("")) {
                            for(int j=0;j<totalFreeUnit.size();j++){
                                record.put(totalFreeUnit.get(j),total[i]);
                            }
                        }
                        else {
                            String tmptotal = total[i];
                            tmptotal =  tmptotal.replace("[{","");
                            tmptotal = tmptotal.replace("}]","");
                            String b = trim(tmptotal);
                            String[] totalFu = b.split(",");
                            int totalFre = totalFu.length;

                            for (int j = 0; j < totalFre; j++) {
                                record.put(totalFreeUnit.get(j), String.valueOf(totalFu[j]));
                            }
                        }
                    }

                    String[] chgbalance = new String[]{record.get("CHG_BALANCE").toString()};
                    ArrayList<String> chgBalance = new ArrayList<>();
                    chgBalance.add("BC1_ACCT_ID");
                    chgBalance.add("BC1_ACCT_BALANCE_ID");
                    chgBalance.add("BC1_BALANCE_TYPE");
                    chgBalance.add("BC1_CUR_BALANCE");
                    chgBalance.add("BC1_CHG_BALANCE");
                    chgBalance.add("BC1_PRE_APPLY_TIME");
                    chgBalance.add("BC1_PRE_EXPIRE_TIME");
                    chgBalance.add("BC1_CUR_EXPIRE_TIME");
                    chgBalance.add("BC1_CURRENCYE_ID");
                    chgBalance.add("BC1_OPER_TYPE");
                    chgBalance.add("BC1_DAILY_BILL_DATE");
                    chgBalance.add("BC1_CHG_FREE_UNIT");

                    int rows = chgbalance.length;
                    for (int i = 0; i < rows; i++) {
                        if (chgbalance[i].equalsIgnoreCase("")) {
                            for(int j=0;j<chgBalance.size();j++){
                                record.put(chgBalance.get(j),chgbalance[i]);
                            }
                        }
                        else {
                            String tmpchg = chgbalance[i];
                            tmpchg =  tmpchg.replace("[{","");
                            tmpchg = tmpchg.replace("}]","");
                            String cb = trim(tmpchg);
                            String[] chrg_balance = cb.split(",");
                            int chgb = chrg_balance.length;

                            for (int j = 0; j < chgb; j++) {
                                record.put(chgBalance.get(j), String.valueOf(chrg_balance[j]));
                            }
                        }
                    }

                    String[] chg = new String[]{record.get("CHG_FREE_UNIT").toString()};
                    ArrayList<String> chgFree = new ArrayList<>();
                    chgFree.add("FC1_FU_OWN_TYPE");
                    chgFree.add("FC1_FU_OWN_ID");
                    chgFree.add("FC1_FREE_UNIT_ID");
                    chgFree.add("FC1_FREE_UNIT_TYPE");
                    chgFree.add("FC1_CUR_AMOUNT");
                    chgFree.add("FC1_CHG_AMOUNT");
                    chgFree.add("FC1_PRE_APPLY_TIME");
                    chgFree.add("FC1_PRE_EXPIRE_TIME");
                    chgFree.add("FC1_CUR_EXPIRE_TIME");
                    chgFree.add("FC1_FU_MEASURE_ID");
                    chgFree.add("FC1_OPER_TYPE");
                    chgFree.add("FC1_OFFERING_ID");
                    chgFree.add("FC1_PURCHASE_SEQ");
                    chgFree.add("FC1_ACCT_ID");
                    chgFree.add("FC1_ACCT_PAID_TYPE");

                    int cfu = chg.length;
                    for (int i = 0; i < cfu; i++) {
                        if (chg[i].equalsIgnoreCase("")) {
                            for(int j=0;j<chgFree.size();j++){
                                record.put(chgFree.get(j),chg[i]);
                            }
                        }
                        else {
                            String tmpchg = chg[i];
                            tmpchg =  tmpchg.replace("[{","");
                            tmpchg = tmpchg.replace("}]","");
                            String c = trim(tmpchg);
                            String[] chgFreeUnit = c.split(",");
                            int chgFreeU = chgFreeUnit.length;

                            for (int j = 0; j < chgFreeU; j++) {
                                record.put(chgFree.get(j), String.valueOf(chgFreeUnit[j]));
                            }
                        }
                    }

                    String[] bonus = new String[]{record.get("BONUS_FUND").toString()};
                    ArrayList<String> bonusFund = new ArrayList<>();
                    bonusFund.add("BD1_ACCT_ID ");
                    bonusFund.add("BD1_ACCT_BALANCE_ID ");
                    bonusFund.add("BD1_BALANCE_TYPE ");
                    bonusFund.add("BD1_BONUS_AMOUNT ");
                    bonusFund.add("BD1_CURRENT_BALANCE");
                    bonusFund.add("BD1_PRE_APPLY_TIME ");
                    bonusFund.add("BD1_PRE_EXPIRE_TIME ");
                    bonusFund.add("BD1_CUR_EXPIRE_TIME ");
                    bonusFund.add("BD1_CURRENCY_ID");
                    bonusFund.add("BD1_OPER_TYPE");
                    bonusFund.add("BD1_PROMO_TYPE");
                    bonusFund.add("BD1_PROMO_ID");

                    int bonus1 = bonus.length;
                    for (int i = 0; i < bonus1; i++) {
                        if (bonus[i].equalsIgnoreCase("")) {
                            for(int j=0;j<bonusFund.size();j++){
                                record.put(bonusFund.get(j),bonus[i]);
                            }
                        }
                        else {
                            String tmpbonus = bonus[i];
                            tmpbonus =  tmpbonus.replace("[{","");
                            tmpbonus = tmpbonus.replace("}]","");
                            String c = trim(tmpbonus);
                            String[] bonusf = c.split(",");
                            int bonusFun = bonusf.length;

                            for (int j = 0; j < bonusFun; j++) {
                                record.put(bonusFund.get(j), String.valueOf(bonusf[j]));
                            }
                        }
                    }

                    String[] bonusFree = new String[]{record.get("BONUS_FREE_UNIT").toString()};
                    ArrayList<String> bonusFreeUnit = new ArrayList<>();
                    bonusFreeUnit.add("FR1_FU_OWN_TYPE");
                    bonusFreeUnit.add("FR1_FU_OWN_ID");
                    bonusFreeUnit.add("FR1_FREE_UNIT_TYPE");
                    bonusFreeUnit.add("FR1_FREE_UNIT_ID");
                    bonusFreeUnit.add("FR1_BONUS_AMOUNT");
                    bonusFreeUnit.add("FR1_CURRENT_AMOUNT");
                    bonusFreeUnit.add("FR1_PRE_APPLY_TIME");
                    bonusFreeUnit.add("FR1_PRE_EXPIRE_TIME");
                    bonusFreeUnit.add("FR1_CUR_EXPIRE_TIME");
                    bonusFreeUnit.add("FR1_FU_MEASURE_ID");
                    bonusFreeUnit.add("FR1_OPER_TYPE");
                    bonusFreeUnit.add("FR1_PROMO_TYPE");
                    bonusFreeUnit.add("FR1_PROMO_ID");

                    int bonusF = bonusFree.length;
                    for (int i = 0; i < bonusF; i++) {
                        if (bonusFree[i].equalsIgnoreCase("")) {
                            for(int j=0;j<bonusFreeUnit.size();j++){
                                record.put(bonusFreeUnit.get(j),bonusFree[i]);
                            }
                        }
                        else {
                            String tmpbonusFree = bonus[i];
                            tmpbonusFree =  tmpbonusFree.replace("[{","");
                            tmpbonusFree = tmpbonusFree.replace("}]","");
                            String d = trim(tmpbonusFree);
                            String[] bonusfree = d.split(",");
                            int bonusFreeUn = bonusfree.length;

                            for (int j = 0; j < bonusFreeUn; j++) {
                                record.put(bonusFreeUnit.get(j), String.valueOf(bonusfree[j]));
                            }
                        }
                    }

                    String[] subLifecycle = new String[]{record.get("SUB_LIFECYCLE").toString()};
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

                    int sub = subLifecycle.length;
                    for (int i = 0; i < sub; i++) {
                        if (subLifecycle[i].equalsIgnoreCase("")) {
                            for(int j=0;j<bonusFreeUnit.size();j++){
                                record.put(subLife.get(j),subLifecycle[i]);
                            }
                        }
                        else {
                            String tmpsub = subLifecycle[i];
                            tmpsub =  tmpsub.replace("[{","");
                            tmpsub = tmpsub.replace("}]","");
                            String d = trim(tmpsub);
                            String[] sublife = d.split(",");
                            int sub_life = sublife.length;

                            for (int j = 0; j < sub_life; j++) {
                                record.put(subLife.get(j), String.valueOf(sublife[j]));
                            }
                        }
                    }

                    String[] additional_info = new String[]{record.get("ADDITIONAL_INFO").toString()};
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

                    int add = additional_info.length;
                    for (int i = 0; i < add; i++) {
                        if (additional_info[i].equalsIgnoreCase("")) {
                            for(int j=0;j<additional.size();j++){
                                record.put(additional.get(j),additional_info[i]);
                            }
                        }
                        else {
                            String tmpadd = additional_info[i];
                            tmpadd =  tmpadd.replace("[{","");
                            tmpadd = tmpadd.replace("}]","");
                            String additinal = trim(tmpadd);
                            String[] additionalInfo = additinal.split(",");
                            int additinalinfo = additionalInfo.length;

                            for (int j = 0; j < additinalinfo; j++) {
                                record.put(additional.get(j), String.valueOf(additionalInfo[j]));
                            }
                        }
                    }

                    String[] loan_info = new String[]{record.get("LOAN_INFO").toString()};
                    ArrayList<String> loan = new ArrayList<>();
                    loan.add("LOAN_AMOUNT");
                    loan.add("LOAN_POUNDAGE");
                    loan.add("LOAN_PENALTY");

                    int loanin = loan_info.length;
                    for (int i = 0; i < loanin; i++) {
                        if (loan_info[i].equalsIgnoreCase("")) {
                            for(int j=0;j<loan.size();j++){
                                record.put(loan.get(j),loan_info[i]);
                            }
                        }
                        else {
                            String tmploan = loan_info[i];
                            tmploan =  tmploan.replace("[{","");
                            tmploan = tmploan.replace("}]","");
                            String loaninf = trim(tmploan);
                            String[] loanIn = loaninf.split(",");
                            int loanInfo = loanIn.length;

                            for (int j = 0; j < loanInfo; j++) {
                                record.put(loan.get(j), String.valueOf(loanIn[j]));
                            }
                        }
                    }

                    String[] tax = new String[]{record.get("TAX_INFO").toString()};
                    ArrayList<String> taxInfo = new ArrayList<>();
                    taxInfo.add("TAX_CODE");
                    taxInfo.add("TAX_AMOUNT");
                    taxInfo.add("TAX_PRICE_FLAG");

                    int taxI = tax.length;
                    for (int i = 0; i < taxI; i++) {
                        if (tax[i].equalsIgnoreCase("")) {
                            for(int j=0;j<taxInfo.size();j++){
                                record.put(taxInfo.get(j),tax[i]);
                            }
                        }
                        else {
                            String taxIn = tax[i];
                            taxIn =  taxIn.replace("[{","");
                            taxIn = taxIn.replace("}]","");
                            String t = trim(taxIn);
                            String[] taxin = t.split(",");
                            int taxinf = taxin.length;

                            for (int j = 0; j < taxinf; j++) {
                                record.put(taxInfo.get(j), String.valueOf(taxin[j]));
                            }
                        }
                    }

                    record.put("fileName", metadata.decompFileName);
                    record.put("REC_SEQ_NO_X", recCount);
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
        while (value.charAt(len - end) == '}')
            end++;

        return value.substring(st, len - end + 1);
    }

    private void processRecord(LinkedHashMap<String, Object> record, IEnrichment enrichment) throws
            Exception {
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

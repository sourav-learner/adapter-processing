package com.gamma.skybase.build.server.etl.decoder.cbs_adj;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class CbsAdjRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsAdjEnrichmentUtil tx = CbsAdjEnrichmentUtil.of(record);

//        SERVED_MSISDN
        String priIdentity = tx.getValue("PRI_IDENTITY");
        if (priIdentity != null) {
            String servedMsisdn;
            if (priIdentity.length() < 12) {
                servedMsisdn = "966" + priIdentity;
            } else {
                servedMsisdn = priIdentity;
            }
            record.put("SERVED_MSISDN", servedMsisdn);
        }

//        RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

//        ADJUST_AMT
        Optional<String> adjustAmt = tx.getAdjustAmt();
        adjustAmt.ifPresent(s -> record.put("ADJUSTAMT", s));

//        DEBIT_AMOUNT
        Optional<String> debitAmount = tx.getDebitAmount();
        debitAmount.ifPresent(s -> record.put("DEBITAMOUNT", s));

//        UN_DEBIT_AMOUNT
        Optional<String> unDebitAmount = tx.getUnDebitAmount();
        unDebitAmount.ifPresent(s -> record.put("UNDEBIT_AMOUNT", s));

//        DEBIT_FROM_PREPAID
        Optional<String> debitFromPrepaid = tx.getDebitFromPrepaid();
        debitFromPrepaid.ifPresent(s -> record.put("DEBITFROM_PREPAID", s));

//        DEBIT_FROM_ADVANCE_PREPAID
        Optional<String> debitFromAdvancePrepaid = tx.getDebitFromAdvancePrepaid();
        debitFromAdvancePrepaid.ifPresent(s -> record.put("DEBITFROM_ADVANCE_PREPAID", s));

//        DEBIT_FROM_POSTPAID
        Optional<String> debitFromPostpaid = tx.getDebitFromPostpaid();
        debitFromPostpaid.ifPresent(s -> record.put("DEBITFROM_POSTPAID", s));

//        DEBIT_FROM_ADVANCE_POSTPAID
        Optional<String> debitFromAdvancedPostpaid = tx.getDebitFromAdvancedPostpaid();
        debitFromAdvancedPostpaid.ifPresent(s -> record.put("DEBITFROM_ADVANCE_POSTPAID", s));

//        DEBIT_FROM_CREDIT_POSTPAID
        Optional<String> debitFromCreditPostpaid = tx.getDebitFromCreditPostpaid();
        debitFromCreditPostpaid.ifPresent(s -> record.put("DEBITFROM_CREDIT_POSTPAID", s));

//        TOTAL_TAX
        Optional<String> totalTax = tx.getTotalTax();
        totalTax.ifPresent(s -> record.put("TOTALTAX", s));

//        BC1_CUR_BALANCE
        Optional<String> bc1CurBalance = tx.getBC1CurBalance();
        bc1CurBalance.ifPresent(s -> record.put("BC1CUR_BALANCE", s));

//        BC1_CHG_BALANCE
        Optional<String> bc1ChgBalance = tx.getBC1ChgBalance();
        bc1ChgBalance.ifPresent(s -> record.put("BC1CHG_BALANCE", s));

//        BC2_CUR_BALANCE
        Optional<String> bc2CurBalance = tx.getBC2CurBalance();
        bc2CurBalance.ifPresent(s -> record.put("BC2CUR_BALANCE", s));

//        BC2_CHG_BALANCE
        Optional<String> bc2ChgBalance = tx.getBC2ChgBalance();
        bc2ChgBalance.ifPresent(s -> record.put("BC2CHG_BALANCE", s));

//        BC3_CUR_BALANCE
        Optional<String> bc3CurBalance = tx.getBC3CurBalance();
        bc3CurBalance.ifPresent(s -> record.put("BC3CUR_BALANCE", s));

//        BC3_CHG_BALANCE
        Optional<String> bc3ChgBalance = tx.getBC3ChgBalance();
        bc3ChgBalance.ifPresent(s -> record.put("BC3CHG_BALANCE", s));

//        BC4_CUR_BALANCE
        Optional<String> bc4CurBalance = tx.getBC4CurBalance();
        bc4CurBalance.ifPresent(s -> record.put("BC4CUR_BALANCE", s));

//        BC4_CHG_BALANCE
        Optional<String> bc4ChgBalance = tx.getBC4ChgBalance();
        bc4ChgBalance.ifPresent(s -> record.put("BC4CHG_BALANCE", s));

//        BC5_CUR_BALANCE
        Optional<String> bc5CurBalance = tx.getBC5CurBalance();
        bc5CurBalance.ifPresent(s -> record.put("BC5CUR_BALANCE", s));

//        BC5_CHG_BALANCE
        Optional<String> bc5ChgBalance = tx.getBC5ChgBalance();
        bc5ChgBalance.ifPresent(s -> record.put("BC5CHG_BALANCE", s));

//        BC6_CUR_BALANCE
        Optional<String> bc6CurBalance = tx.getBC6CurBalance();
        bc6CurBalance.ifPresent(s -> record.put("BC6CUR_BALANCE", s));

//        BC6_CHG_BALANCE
        Optional<String> bc6ChgBalance = tx.getBC6ChgBalance();
        bc6ChgBalance.ifPresent(s -> record.put("BC6CHG_BALANCE", s));

//        BC7_CUR_BALANCE
        Optional<String> bc7CurBalance = tx.getBC7CurBalance();
        bc7CurBalance.ifPresent(s -> record.put("BC7CUR_BALANCE", s));

//        BC7_CHG_BALANCE
        Optional<String> bc7ChgBalance = tx.getBC7ChgBalance();
        bc7ChgBalance.ifPresent(s -> record.put("BC7CHG_BALANCE", s));

//        BC8_CUR_BALANCE
        Optional<String> bc8CurBalance = tx.getBC8CurBalance();
        bc8CurBalance.ifPresent(s -> record.put("BC8CUR_BALANCE", s));

//        BC8_CHG_BALANCE
        Optional<String> bc8ChgBalance = tx.getBC8ChgBalance();
        bc8ChgBalance.ifPresent(s -> record.put("BC8CHG_BALANCE", s));

//        BC9_CUR_BALANCE
        Optional<String> bc9CurBalance = tx.getBC9CurBalance();
        bc9CurBalance.ifPresent(s -> record.put("BC9CUR_BALANCE", s));

//        BC9_CHG_BALANCE
        Optional<String> bc9ChgBalance = tx.getBC9ChgBalance();
        bc9ChgBalance.ifPresent(s -> record.put("BC9CHG_BALANCE", s));

//        BC10_CUR_BALANCE
        Optional<String> bc10CurBalance = tx.getBC10CurBalance();
        bc10CurBalance.ifPresent(s -> record.put("BC10CUR_BALANCE", s));

//        BC10_CHG_BALANCE
        Optional<String> bc10ChgBalance = tx.getBC10ChgBalance();
        bc10ChgBalance.ifPresent(s -> record.put("BC10CHG_BALANCE", s));

//        FC1_CUR_AMOUNT
        Optional<String> fc1CurAmount= tx.getFC1CurAmount();
        fc1CurAmount.ifPresent(s -> record.put("FC1CUR_AMOUNT", s));

//        FC1_CHG_AMOUNT
        Optional<String> fc1ChgAmount = tx.getFC1ChgAmount();
        fc1ChgAmount.ifPresent(s -> record.put("FC1CHG_AMOUNT", s));

//        FC2_CUR_AMOUNT
        Optional<String> fc2CurAmount= tx.getFC2CurAmount();
        fc2CurAmount.ifPresent(s -> record.put("FC2CUR_AMOUNT", s));

//        FC2_CHG_AMOUNT
        Optional<String> fc2ChgAmount = tx.getFC2ChgAmount();
        fc2ChgAmount.ifPresent(s -> record.put("FC2CHG_AMOUNT", s));

//        FC3_CUR_AMOUNT
        Optional<String> fc3CurAmount= tx.getFC3CurAmount();
        fc3CurAmount.ifPresent(s -> record.put("FC3CUR_AMOUNT", s));

//        FC3_CHG_AMOUNT
        Optional<String> fc3ChgAmount = tx.getFC3ChgAmount();
        fc3ChgAmount.ifPresent(s -> record.put("FC3CHG_AMOUNT", s));

//        FC4_CUR_AMOUNT
        Optional<String> fc4CurAmount= tx.getFC4CurAmount();
        fc4CurAmount.ifPresent(s -> record.put("FC4CUR_AMOUNT", s));

//        FC4_CHG_AMOUNT
        Optional<String> fc4ChgAmount = tx.getFC4ChgAmount();
        fc4ChgAmount.ifPresent(s -> record.put("FC4CHG_AMOUNT", s));

//        FC5_CUR_AMOUNT
        Optional<String> fc5CurAmount= tx.getFC5CurAmount();
        fc5CurAmount.ifPresent(s -> record.put("FC5CUR_AMOUNT", s));

//        FC5_CHG_AMOUNT
        Optional<String> fc5ChgAmount = tx.getFC5ChgAmount();
        fc5ChgAmount.ifPresent(s -> record.put("FC5CHG_AMOUNT", s));

//        FC6_CUR_AMOUNT
        Optional<String> fc6CurAmount= tx.getFC6CurAmount();
        fc6CurAmount.ifPresent(s -> record.put("FC6CUR_AMOUNT", s));

//        FC6_CHG_AMOUNT
        Optional<String> fc6ChgAmount = tx.getFC6ChgAmount();
        fc6ChgAmount.ifPresent(s -> record.put("FC6CHG_AMOUNT", s));

//        FC7_CUR_AMOUNT
        Optional<String> fc7CurAmount= tx.getFC7CurAmount();
        fc7CurAmount.ifPresent(s -> record.put("FC7CUR_AMOUNT", s));

//        FC7_CHG_AMOUNT
        Optional<String> fc7ChgAmount = tx.getFC7ChgAmount();
        fc7ChgAmount.ifPresent(s -> record.put("FC7CHG_AMOUNT", s));

//        FC8_CUR_AMOUNT
        Optional<String> fc8CurAmount= tx.getFC8CurAmount();
        fc8CurAmount.ifPresent(s -> record.put("FC8CUR_AMOUNT", s));

//        FC8_CHG_AMOUNT
        Optional<String> fc8ChgAmount = tx.getFC8ChgAmount();
        fc8ChgAmount.ifPresent(s -> record.put("FC8CHG_AMOUNT", s));

//        FC9_CUR_AMOUNT
        Optional<String> fc9CurAmount= tx.getFC9CurAmount();
        fc9CurAmount.ifPresent(s -> record.put("FC9CUR_AMOUNT", s));

//        FC9_CHG_AMOUNT
        Optional<String> fc9ChgAmount = tx.getFC9ChgAmount();
        fc9ChgAmount.ifPresent(s -> record.put("FC9CHG_AMOUNT", s));

//        FC10_CUR_AMOUNT
        Optional<String> fc10CurAmount= tx.getFC10CurAmount();
        fc10CurAmount.ifPresent(s -> record.put("FC10CUR_AMOUNT", s));

//        FC10_CHG_AMOUNT
        Optional<String> fc10ChgAmount = tx.getFC10ChgAmount();
        fc10ChgAmount.ifPresent(s -> record.put("FC10CHG_AMOUNT", s));

//        BD1_BONUS_AMOUNT
        Optional<String> bd1BonusAmt = tx.getBD1BonusAmt();
        bd1BonusAmt.ifPresent(s -> record.put("BD1BONUS_AMOUNT", s));

//        BD1_CURRENT_BALANCE
        Optional<String> bd1CurBalance= tx.getBD1CurBalance();
        bd1CurBalance.ifPresent(s -> record.put("BD1CURRENT_BALANCE", s));

//        BD2_BONUS_AMOUNT
        Optional<String> bd2BonusAmt = tx.getBD2BonusAmt();
        bd2BonusAmt.ifPresent(s -> record.put("BD2BONUS_AMOUNT", s));

//        BD2_CURRENT_BALANCE
        Optional<String> bd2CurBalance= tx.getBD2CurBalance();
        bd2CurBalance.ifPresent(s -> record.put("BD2CURRENT_BALANCE", s));

//        BD3_BONUS_AMOUNT
        Optional<String> bd3BonusAmt = tx.getBD3BonusAmt();
        bd3BonusAmt.ifPresent(s -> record.put("BD3BONUS_AMOUNT", s));

//        BD3_CURRENT_BALANCE
        Optional<String> bd3CurBalance= tx.getBD3CurBalance();
        bd3CurBalance.ifPresent(s -> record.put("BD3CURRENT_BALANCE", s));

//        BD4_BONUS_AMOUNT
        Optional<String> bd4BonusAmt = tx.getBD4BonusAmt();
        bd4BonusAmt.ifPresent(s -> record.put("BD4BONUS_AMOUNT", s));

//        BD4_CURRENT_BALANCE
        Optional<String> bd4CurBalance= tx.getBD4CurBalance();
        bd4CurBalance.ifPresent(s -> record.put("BD4CURRENT_BALANCE", s));

//        BD5_BONUS_AMOUNT
        Optional<String> bd5BonusAmt = tx.getBD5BonusAmt();
        bd5BonusAmt.ifPresent(s -> record.put("BD5BONUS_AMOUNT", s));

//        BD5_CURRENT_BALANCE
        Optional<String> bd5CurBalance= tx.getBD5CurBalance();
        bd5CurBalance.ifPresent(s -> record.put("BD5CURRENT_BALANCE", s));

//        BD6_BONUS_AMOUNT
        Optional<String> bd6BonusAmt = tx.getBD6BonusAmt();
        bd6BonusAmt.ifPresent(s -> record.put("BD6BONUS_AMOUNT", s));

//        BD6_CURRENT_BALANCE
        Optional<String> bd6CurBalance= tx.getBD6CurBalance();
        bd6CurBalance.ifPresent(s -> record.put("BD6CURRENT_BALANCE", s));

//        BD7_BONUS_AMOUNT
        Optional<String> bd7BonusAmt = tx.getBD7BonusAmt();
        bd7BonusAmt.ifPresent(s -> record.put("BD7BONUS_AMOUNT", s));

//        BD7_CURRENT_BALANCE
        Optional<String> bd7CurBalance= tx.getBD7CurBalance();
        bd7CurBalance.ifPresent(s -> record.put("BD7CURRENT_BALANCE", s));

//        BD8_BONUS_AMOUNT
        Optional<String> bd8BonusAmt = tx.getBD8BonusAmt();
        bd8BonusAmt.ifPresent(s -> record.put("BD8BONUS_AMOUNT", s));

//        BD8_CURRENT_BALANCE
        Optional<String> bd8CurBalance= tx.getBD8CurBalance();
        bd8CurBalance.ifPresent(s -> record.put("BD8CURRENT_BALANCE", s));

//        BD9_BONUS_AMOUNT
        Optional<String> bd9BonusAmt = tx.getBD9BonusAmt();
        bd9BonusAmt.ifPresent(s -> record.put("BD9BONUS_AMOUNT", s));

//        BD9_CURRENT_BALANCE
        Optional<String> bd9CurBalance= tx.getBD9CurBalance();
        bd9CurBalance.ifPresent(s -> record.put("BD9CURRENT_BALANCE", s));

//        BD10_BONUS_AMOUNT
        Optional<String> bd10BonusAmt = tx.getBD10BonusAmt();
        bd10BonusAmt.ifPresent(s -> record.put("BD10BONUS_AMOUNT", s));

//        BD10_CURRENT_BALANCE
        Optional<String> bd10CurBalance= tx.getBD10CurBalance();
        bd10CurBalance.ifPresent(s -> record.put("BD10CURRENT_BALANCE", s));

//        FR1_BONUS_AMOUNT
        Optional<String> fr1BonusAmt = tx.getFR1BonusAmt();
        fr1BonusAmt.ifPresent(s -> record.put("FR1BONUS_AMOUNT", s));

//        FR1_CURRENT_AMOUNT
        Optional<String> fr1CurAmt= tx.getFR1CurBalance();
        fr1CurAmt.ifPresent(s -> record.put("FR1CURRENT_AMOUNT", s));

//        FR2_BONUS_AMOUNT
        Optional<String> fr2BonusAmt = tx.getFR2BonusAmt();
        fr2BonusAmt.ifPresent(s -> record.put("FR2BONUS_AMOUNT", s));

//        FR2_CURRENT_AMOUNT
        Optional<String> fr2CurAmt= tx.getFR2CurBalance();
        fr2CurAmt.ifPresent(s -> record.put("FR2CURRENT_AMOUNT", s));

//        FR3_BONUS_AMOUNT
        Optional<String> fr3BonusAmt = tx.getFR3BonusAmt();
        fr3BonusAmt.ifPresent(s -> record.put("FR3BONUS_AMOUNT", s));

//        FR3_CURRENT_AMOUNT
        Optional<String> fr3CurAmt= tx.getFR3CurBalance();
        fr3CurAmt.ifPresent(s -> record.put("FR3CURRENT_AMOUNT", s));

//        FR4_BONUS_AMOUNT
        Optional<String> fr4BonusAmt = tx.getFR4BonusAmt();
        fr4BonusAmt.ifPresent(s -> record.put("FR4BONUS_AMOUNT", s));

//        FR4_CURRENT_AMOUNT
        Optional<String> fr4CurAmt= tx.getFR4CurBalance();
        fr4CurAmt.ifPresent(s -> record.put("FR4CURRENT_AMOUNT", s));

//        FR5_BONUS_AMOUNT
        Optional<String> fr5BonusAmt = tx.getFR5BonusAmt();
        fr5BonusAmt.ifPresent(s -> record.put("FR5BONUS_AMOUNT", s));

//        FR5_CURRENT_AMOUNT
        Optional<String> fr5CurAmt= tx.getFR5CurBalance();
        fr5CurAmt.ifPresent(s -> record.put("FR5CURRENT_AMOUNT", s));

//        FR6_BONUS_AMOUNT
        Optional<String> fr6BonusAmt = tx.getFR6BonusAmt();
        fr6BonusAmt.ifPresent(s -> record.put("FR6BONUS_AMOUNT", s));

//        FR6_CURRENT_AMOUNT
        Optional<String> fr6CurAmt= tx.getFR6CurBalance();
        fr6CurAmt.ifPresent(s -> record.put("FR6CURRENT_AMOUNT", s));

//        FR7_BONUS_AMOUNT
        Optional<String> fr7BonusAmt = tx.getFR7BonusAmt();
        fr7BonusAmt.ifPresent(s -> record.put("FR7BONUS_AMOUNT", s));

//        FR7_CURRENT_AMOUNT
        Optional<String> fr7CurAmt= tx.getFR7CurBalance();
        fr7CurAmt.ifPresent(s -> record.put("FR7CURRENT_AMOUNT", s));

//        FR8_BONUS_AMOUNT
        Optional<String> fr8BonusAmt = tx.getFR8BonusAmt();
        fr8BonusAmt.ifPresent(s -> record.put("FR8BONUS_AMOUNT", s));

//        FR8_CURRENT_AMOUNT
        Optional<String> fr8CurAmt= tx.getFR8CurBalance();
        fr8CurAmt.ifPresent(s -> record.put("FR8CURRENT_AMOUNT", s));

//        FR9_BONUS_AMOUNT
        Optional<String> fr9BonusAmt = tx.getFR9BonusAmt();
        fr9BonusAmt.ifPresent(s -> record.put("FR9BONUS_AMOUNT", s));

//        FR9_CURRENT_AMOUNT
        Optional<String> fr9CurAmt= tx.getFR9CurBalance();
        fr9CurAmt.ifPresent(s -> record.put("FR9CURRENT_AMOUNT", s));

//        FR10_BONUS_AMOUNT
        Optional<String> fr10BonusAmt = tx.getFR10BonusAmt();
        fr10BonusAmt.ifPresent(s -> record.put("FR10BONUS_AMOUNT", s));

//        FR10_CURRENT_AMOUNT
        Optional<String> fr10CurAmt= tx.getFR10CurBalance();
        fr10CurAmt.ifPresent(s -> record.put("FR10CURRENT_AMOUNT", s));

//        LOAN_AMOUNT
        Optional<String> loanAmt= tx.getLoanAmt();
        loanAmt.ifPresent(s -> record.put("LOANAMOUNT", s));

//        LOAN_POUNDAGE
        Optional<String> loanPoundage= tx.getLoanPoundage();
        loanPoundage.ifPresent(s -> record.put("LOANPOUNDAGE", s));

//        LOAN_PENALTY
        Optional<String> loanPenalty= tx.getLoanPenalty();
        loanPenalty.ifPresent(s -> record.put("LOANPENALTY", s));

//        TAX_AMOUNT
        Optional<String> taxAmt= tx.getTaxAmt();
        taxAmt.ifPresent(s -> record.put("TAXAMOUNT", s));

//        STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("ADJUSTMENT_STATUS", s));

//        ENTRY_DATE
        Optional<String> starTime = tx.getStartTime("ENTRY_DATE");
        starTime.ifPresent(s -> {
            record.put("ENTRYDATE", s);
            record.put("XDR_DATE", s);
        });

//        BC1_OPER_TYPE
        Optional<String> BC1OperType = tx.getBC1OperType();
        BC1OperType.ifPresent(s -> {
            record.put("BC1_OPER_TYPE", s);
            record.put("BC2_OPER_TYPE", s);
            record.put("BC3_OPER_TYPE", s);
            record.put("BC4_OPER_TYPE", s);
            record.put("BC5_OPER_TYPE", s);
            record.put("BC6_OPER_TYPE", s);
            record.put("BC7_OPER_TYPE", s);
            record.put("BC8_OPER_TYPE", s);
            record.put("BC9_OPER_TYPE", s);
            record.put("BC10_OPER_TYPE", s);
        });

//        FC1_Fu_Own_Type
        Optional<String> FC1FuOwnType = tx.getFC1FUOwnType();
        FC1FuOwnType.ifPresent(s -> {
            record.put("FC1_FU_OWN_TYPE", s);
            record.put("FC2_FU_OWN_TYPE", s);
            record.put("FC3_FU_OWN_TYPE", s);
            record.put("FC4_FU_OWN_TYPE", s);
            record.put("FC5_FU_OWN_TYPE", s);
            record.put("FC6_FU_OWN_TYPE", s);
            record.put("FC7_FU_OWN_TYPE", s);
            record.put("FC8_FU_OWN_TYPE", s);
            record.put("FC9_FU_OWN_TYPE", s);
            record.put("FC10_FU_OWN_TYPE", s);
        });

//        FC1_OPER_TYPE
        Optional<String> FC1OperType = tx.getFC1OperType();
        FC1OperType.ifPresent(s -> {
            record.put("FC1_OPER_TYPE", s);
            record.put("FC2_OPER_TYPE", s);
            record.put("FC3_OPER_TYPE",s);
            record.put("FC4_OPER_TYPE",s);
            record.put("FC5_OPER_TYPE",s);
            record.put("FC6_OPER_TYPE",s);
            record.put("FC7_OPER_TYPE",s);
            record.put("FC8_OPER_TYPE",s);
            record.put("FC9_OPER_TYPE",s);
            record.put("FC10_OPER_TYPE",s);
        });

//        BD1_OPER_TYPE
        Optional<String> BD1OperType = tx.getBD1OperType();
        BD1OperType.ifPresent(s -> {
            record.put("BD1_OPER_TYPE", s);
            record.put("BD2_OPER_TYPE", s);
            record.put("BD3_OPER_TYPE",s);
            record.put("BD4_OPER_TYPE",s);
            record.put("BD5_OPER_TYPE",s);
            record.put("BD6_OPER_TYPE",s);
            record.put("BD7_OPER_TYPE",s);
            record.put("BD8_OPER_TYPE",s);
            record.put("BD9_OPER_TYPE",s);
            record.put("BD10_OPER_TYPE",s);
        });

//        FR1_FU_OWN_TYPE
        Optional<String> FR1FuOwnType = tx.getFR1FUOwnType();
        FR1FuOwnType.ifPresent(s -> {
            record.put("FR1_FU_OWN_TYPE", s);
            record.put("FR2_FU_OWN_TYPE", s);
            record.put("FR3_FU_OWN_TYPE", s);
            record.put("FR4_FU_OWN_TYPE", s);
            record.put("FR5_FU_OWN_TYPE", s);
            record.put("FR6_FU_OWN_TYPE", s);
            record.put("FR7_FU_OWN_TYPE", s);
            record.put("FR8_FU_OWN_TYPE", s);
            record.put("FR9_FU_OWN_TYPE", s);
            record.put("FR10_FU_OWN_TYPE", s);
        });

//        FR1_OPER_TYPE
        Optional<String> FR1OperType = tx.getFR1OperType();
        FR1OperType.ifPresent(s -> {
            record.put("FR1_OPERTYPE", s);
            record.put("FR2_OPERTYPE", s);
            record.put("FR3_OPERTYPE", s);
            record.put("FR4_OPERTYPE", s);
            record.put("FR5_OPERTYPE", s);
            record.put("FR6_OPERTYPE", s);
            record.put("FR7_OPERTYPE", s);
            record.put("FR8_OPERTYPE", s);
            record.put("FR9_OPERTYPE", s);
            record.put("FR10_OPERTYPE", s);
        });

//        OLD_STATUS
        Optional<String> oldStatus = tx.getOldStatus();
        oldStatus.ifPresent(s ->
            record.put("OLD_STATUS", s));

//        CURRENT_STATUS
        Optional<String> currentStatus = tx.getCurrentStatus();
        currentStatus.ifPresent(s -> record.put("CURRENT_STATUS", s));

//      SERVED_TYPE
        Optional<String> payType = tx.getServedType();
        payType.ifPresent(s -> record.put("SERVED_TYPE", s));

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
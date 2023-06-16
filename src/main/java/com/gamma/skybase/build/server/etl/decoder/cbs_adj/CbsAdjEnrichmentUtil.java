package com.gamma.skybase.build.server.etl.decoder.cbs_adj;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsAdjEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;
    private CbsAdjEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsAdjEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsAdjEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    Long callDuration;

    public Optional<String> getCallDuration(String field) {
        String s = getValue(field);
        callDuration = 0L;
        try {
            if (s != null)
                callDuration = Long.parseLong(s);
            return Optional.of(String.valueOf(callDuration));
        } catch (NumberFormatException e) { // Ignore non numbers
        }
        return Optional.empty();
    }

    Date callStartTime;
    String genFullDate;

    public Optional<String> getStartTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callStartTime = sdfS.get().parse(s);
                genFullDate = fullDate.get().format(callStartTime);
                return Optional.of(sdfT.get().format(callStartTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    Date callEndTime;

    public Optional<String> getEndTime(String time, String callEventDuration) {
        if (callDuration == null) getCallDuration(callEventDuration);
        if (callStartTime == null) getStartTime(time);

        if (callStartTime == null || callDuration == null) return Optional.empty();

        callEndTime = new Date(callStartTime.getTime() / 1000 + callDuration);
        return Optional.of(sdfT.get().format(callEndTime));
    }

    String resultCode;

    public Optional<String> getResultCode() {
        resultCode = getValue("RESULT_CODE");
        if (resultCode != null) {
            switch (resultCode) {
                case "0":
                    resultCode = "SUCCESSFUL";
                    break;
                default:
                    resultCode = "UNSUCCESSFUL";
                    break;
            }
        }
        if (resultCode != null)
            return Optional.of(resultCode);
        return Optional.empty();
    }

    String adjustAmt;

    public Optional<String> getAdjustAmt() {
        adjustAmt = getValue("ADJUST_AMT");
        if (adjustAmt != null) {
            int adjAmt = Integer.parseInt(adjustAmt)/10000;
            adjustAmt = String.valueOf(adjAmt);
        }
        if (adjustAmt != null)
            return Optional.of(adjustAmt);
        return Optional.empty();
    }

    String status;

    public Optional<String> getStatus() {
        status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "F":
                    status = "FINISHED";
                    break;
                case "R":
                    status = "REVERSED";
                    break;
                default:
                    status = "-99";
                    break;
            }
        }
        if (status != null)
            return Optional.of(status);
        return Optional.empty();
    }

    String debitAmount;

    public Optional<String> getDebitAmount() {
        debitAmount = getValue("DEBIT_AMOUNT");
        if (debitAmount != null) {
            int adjAmt = Integer.parseInt(debitAmount)/10000;
            debitAmount = String.valueOf(adjAmt);
        }
        if (debitAmount != null)
            return Optional.of(debitAmount);
        return Optional.empty();
    }

    String unDebitAmount;

    public Optional<String> getUnDebitAmount() {
        unDebitAmount = getValue("UN_DEBIT_AMOUNT");
        if (unDebitAmount != null) {
            int adjAmt = Integer.parseInt(unDebitAmount)/10000;
            unDebitAmount = String.valueOf(adjAmt);
        }
        if (unDebitAmount != null)
            return Optional.of(unDebitAmount);
        return Optional.empty();
    }

    String debitFromPrepaid;

    public Optional<String> getDebitFromPrepaid() {
        debitFromPrepaid = getValue("DEBIT_FROM_PREPAID");
        if (debitFromPrepaid != null) {
            int adjAmt = Integer.parseInt(debitFromPrepaid)/10000;
            debitFromPrepaid = String.valueOf(adjAmt);
        }
        if (debitFromPrepaid != null)
            return Optional.of(debitFromPrepaid);
        return Optional.empty();
    }

    String debitFromAdvancePrepaid;

    public Optional<String> getDebitFromAdvancePrepaid() {
        debitFromAdvancePrepaid = getValue("DEBIT_FROM_ADVANCE_PREPAID");
        if (debitFromAdvancePrepaid != null) {
            int adjAmt = Integer.parseInt(debitFromAdvancePrepaid)/10000;
            debitFromAdvancePrepaid = String.valueOf(adjAmt);
        }
        if (debitFromAdvancePrepaid != null)
            return Optional.of(debitFromAdvancePrepaid);
        return Optional.empty();
    }

    String debitFromPostpaid;

    public Optional<String> getDebitFromPostpaid() {
        debitFromPostpaid = getValue("DEBIT_FROM_POSTPAID");
        if (debitFromPostpaid != null) {
            int adjAmt = Integer.parseInt(debitFromPostpaid)/10000;
            debitFromPostpaid = String.valueOf(adjAmt);
        }
        if (debitFromPostpaid != null)
            return Optional.of(debitFromPostpaid);
        return Optional.empty();
    }

    String debitFromAdvancedPostpaid;

    public Optional<String> getDebitFromAdvancedPostpaid() {
        debitFromAdvancedPostpaid = getValue("DEBIT_FROM_ADVANCE_POSTPAID");
        if (debitFromAdvancedPostpaid != null) {
            int adjAmt = Integer.parseInt(debitFromAdvancedPostpaid)/10000;
            debitFromAdvancedPostpaid = String.valueOf(adjAmt);
        }
        if (debitFromAdvancedPostpaid != null)
            return Optional.of(debitFromAdvancedPostpaid);
        return Optional.empty();
    }

    String debitFromCreditPostpaid;

    public Optional<String> getDebitFromCreditPostpaid() {
        debitFromCreditPostpaid = getValue("DEBIT_FROM_CREDIT_POSTPAID");
        if (debitFromCreditPostpaid != null) {
            int adjAmt = Integer.parseInt(debitFromCreditPostpaid)/10000;
            debitFromCreditPostpaid = String.valueOf(adjAmt);
        }
        if (debitFromCreditPostpaid != null)
            return Optional.of(debitFromCreditPostpaid);
        return Optional.empty();
    }

    String totalTax;

    public Optional<String> getTotalTax() {
        totalTax = getValue("TOTAL_TAX");
        if (totalTax != null) {
            int adjAmt = Integer.parseInt(totalTax)/10000;
            totalTax = String.valueOf(adjAmt);
        }
        if (totalTax != null)
            return Optional.of(totalTax);
        return Optional.empty();
    }

    String bc1CurBalance;

    public Optional<String> getBC1CurBalance() {
        bc1CurBalance = getValue("BC1_CUR_BALANCE");
        if (bc1CurBalance != null) {
            int adjAmt = Integer.parseInt(bc1CurBalance)/10000;
            bc1CurBalance = String.valueOf(adjAmt);
        }
        if (bc1CurBalance != null)
            return Optional.of(bc1CurBalance);
        return Optional.empty();
    }

    String bc1ChgBalance;

    public Optional<String> getBC1ChgBalance() {
        bc1ChgBalance = getValue("BC1_CHG_BALANCE");
        if (bc1ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc1ChgBalance)/10000;
            bc1ChgBalance = String.valueOf(adjAmt);
        }
        if (bc1ChgBalance != null)
            return Optional.of(bc1ChgBalance);
        return Optional.empty();
    }

    String bc2CurBalance;

    public Optional<String> getBC2CurBalance() {
        bc2CurBalance = getValue("BC2_CUR_BALANCE");
        if (bc2CurBalance != null) {
            int adjAmt = Integer.parseInt(bc2CurBalance)/10000;
            bc2CurBalance = String.valueOf(adjAmt);
        }
        if (bc2CurBalance != null)
            return Optional.of(bc2CurBalance);
        return Optional.empty();
    }

    String bc2ChgBalance;

    public Optional<String> getBC2ChgBalance() {
        bc2ChgBalance = getValue("BC2_CHG_BALANCE");
        if (bc2ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc2ChgBalance)/10000;
            bc2ChgBalance = String.valueOf(adjAmt);
        }
        if (bc2ChgBalance != null)
            return Optional.of(bc2ChgBalance);
        return Optional.empty();
    }

    String bc3CurBalance;

    public Optional<String> getBC3CurBalance() {
        bc3CurBalance = getValue("BC3_CUR_BALANCE");
        if (bc3CurBalance != null) {
            int adjAmt = Integer.parseInt(bc3CurBalance)/10000;
            bc3CurBalance = String.valueOf(adjAmt);
        }
        if (bc3CurBalance != null)
            return Optional.of(bc3CurBalance);
        return Optional.empty();
    }

    String bc3ChgBalance;

    public Optional<String> getBC3ChgBalance() {
        bc3ChgBalance = getValue("BC3_CHG_BALANCE");
        if (bc3ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc3ChgBalance)/10000;
            bc3ChgBalance = String.valueOf(adjAmt);
        }
        if (bc3ChgBalance != null)
            return Optional.of(bc3ChgBalance);
        return Optional.empty();
    }

    String bc4CurBalance;

    public Optional<String> getBC4CurBalance() {
        bc4CurBalance = getValue("BC4_CUR_BALANCE");
        if (bc4CurBalance != null) {
            int adjAmt = Integer.parseInt(bc4CurBalance)/10000;
            bc4CurBalance = String.valueOf(adjAmt);
        }
        if (bc4CurBalance != null)
            return Optional.of(bc4CurBalance);
        return Optional.empty();
    }

    String bc4ChgBalance;

    public Optional<String> getBC4ChgBalance() {
        bc4ChgBalance = getValue("BC4_CHG_BALANCE");
        if (bc4ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc4ChgBalance)/10000;
            bc4ChgBalance = String.valueOf(adjAmt);
        }
        if (bc4ChgBalance != null)
            return Optional.of(bc4ChgBalance);
        return Optional.empty();
    }

    String bc5CurBalance;

    public Optional<String> getBC5CurBalance() {
        bc5CurBalance = getValue("BC5_CUR_BALANCE");
        if (bc5CurBalance != null) {
            int adjAmt = Integer.parseInt(bc5CurBalance)/10000;
            bc5CurBalance = String.valueOf(adjAmt);
        }
        if (bc5CurBalance != null)
            return Optional.of(bc5CurBalance);
        return Optional.empty();
    }

    String bc5ChgBalance;

    public Optional<String> getBC5ChgBalance() {
        bc5ChgBalance = getValue("BC5_CHG_BALANCE");
        if (bc5ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc5ChgBalance)/10000;
            bc5ChgBalance = String.valueOf(adjAmt);
        }
        if (bc5ChgBalance != null)
            return Optional.of(bc5ChgBalance);
        return Optional.empty();
    }

    String bc6CurBalance;

    public Optional<String> getBC6CurBalance() {
        bc6CurBalance = getValue("BC6_CUR_BALANCE");
        if (bc6CurBalance != null) {
            int adjAmt = Integer.parseInt(bc6CurBalance)/10000;
            bc6CurBalance = String.valueOf(adjAmt);
        }
        if (bc6CurBalance != null)
            return Optional.of(bc6CurBalance);
        return Optional.empty();
    }

    String bc6ChgBalance;

    public Optional<String> getBC6ChgBalance() {
        bc6ChgBalance = getValue("BC6_CHG_BALANCE");
        if (bc6ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc6ChgBalance)/10000;
            bc6ChgBalance = String.valueOf(adjAmt);
        }
        if (bc6ChgBalance != null)
            return Optional.of(bc6ChgBalance);
        return Optional.empty();
    }

    String bc7CurBalance;

    public Optional<String> getBC7CurBalance() {
        bc7CurBalance = getValue("BC7_CUR_BALANCE");
        if (bc7CurBalance != null) {
            int adjAmt = Integer.parseInt(bc7CurBalance)/10000;
            bc7CurBalance = String.valueOf(adjAmt);
        }
        if (bc7CurBalance != null)
            return Optional.of(bc7CurBalance);
        return Optional.empty();
    }

    String bc7ChgBalance;

    public Optional<String> getBC7ChgBalance() {
        bc7ChgBalance = getValue("BC7_CHG_BALANCE");
        if (bc7ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc7ChgBalance)/10000;
            bc7ChgBalance = String.valueOf(adjAmt);
        }
        if (bc7ChgBalance != null)
            return Optional.of(bc7ChgBalance);
        return Optional.empty();
    }

    String bc8CurBalance;

    public Optional<String> getBC8CurBalance() {
        bc8CurBalance = getValue("BC8_CUR_BALANCE");
        if (bc8CurBalance != null) {
            int adjAmt = Integer.parseInt(bc8CurBalance)/10000;
            bc8CurBalance = String.valueOf(adjAmt);
        }
        if (bc8CurBalance != null)
            return Optional.of(bc8CurBalance);
        return Optional.empty();
    }

    String bc8ChgBalance;

    public Optional<String> getBC8ChgBalance() {
        bc8ChgBalance = getValue("BC8_CHG_BALANCE");
        if (bc8ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc8ChgBalance)/10000;
            bc8ChgBalance = String.valueOf(adjAmt);
        }
        if (bc8ChgBalance != null)
            return Optional.of(bc8ChgBalance);
        return Optional.empty();
    }

    String bc9CurBalance;

    public Optional<String> getBC9CurBalance() {
        bc9CurBalance = getValue("BC9_CUR_BALANCE");
        if (bc9CurBalance != null) {
            int adjAmt = Integer.parseInt(bc9CurBalance)/10000;
            bc9CurBalance = String.valueOf(adjAmt);
        }
        if (bc9CurBalance != null)
            return Optional.of(bc9CurBalance);
        return Optional.empty();
    }

    String bc9ChgBalance;

    public Optional<String> getBC9ChgBalance() {
        bc9ChgBalance = getValue("BC9_CHG_BALANCE");
        if (bc9ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc9ChgBalance)/10000;
            bc9ChgBalance = String.valueOf(adjAmt);
        }
        if (bc9ChgBalance != null)
            return Optional.of(bc9ChgBalance);
        return Optional.empty();
    }

    String bc10CurBalance;

    public Optional<String> getBC10CurBalance() {
        bc10CurBalance = getValue("BC10_CUR_BALANCE");
        if (bc10CurBalance != null) {
            int adjAmt = Integer.parseInt(bc10CurBalance)/10000;
            bc10CurBalance = String.valueOf(adjAmt);
        }
        if (bc10CurBalance != null)
            return Optional.of(bc10CurBalance);
        return Optional.empty();
    }

    String bc10ChgBalance;

    public Optional<String> getBC10ChgBalance() {
        bc10ChgBalance = getValue("BC10_CHG_BALANCE");
        if (bc10ChgBalance != null) {
            int adjAmt = Integer.parseInt(bc10ChgBalance)/10000;
            bc10ChgBalance = String.valueOf(adjAmt);
        }
        if (bc10ChgBalance != null)
            return Optional.of(bc10ChgBalance);
        return Optional.empty();
    }


    String fc1CurAmount;

    public Optional<String> getFC1CurAmount() {
        fc1CurAmount = getValue("FC1_CUR_AMOUNT");
        if (fc1CurAmount != null) {
            int adjAmt = Integer.parseInt(fc1CurAmount)/10000;
            fc1CurAmount = String.valueOf(adjAmt);
        }
        if (fc1CurAmount != null)
            return Optional.of(fc1CurAmount);
        return Optional.empty();
    }

    String fc1ChgAmount;

    public Optional<String> getFC1ChgAmount() {
        fc1ChgAmount = getValue("FC1_CHG_AMOUNT");
        if (fc1ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc1ChgAmount)/10000;
            fc1ChgAmount = String.valueOf(adjAmt);
        }
        if (fc1ChgAmount != null)
            return Optional.of(fc1ChgAmount);
        return Optional.empty();
    }

    String fc2CurAmount;

    public Optional<String> getFC2CurAmount() {
        fc2CurAmount = getValue("FC2_CUR_AMOUNT");
        if (fc2CurAmount != null) {
            int adjAmt = Integer.parseInt(fc2CurAmount)/30000;
            fc2CurAmount = String.valueOf(adjAmt);
        }
        if (fc2CurAmount != null)
            return Optional.of(fc2CurAmount);
        return Optional.empty();
    }

    String fc2ChgAmount;

    public Optional<String> getFC2ChgAmount() {
        fc2ChgAmount = getValue("FC2_CHG_AMOUNT");
        if (fc2ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc2ChgAmount)/30000;
            fc2ChgAmount = String.valueOf(adjAmt);
        }
        if (fc2ChgAmount != null)
            return Optional.of(fc2ChgAmount);
        return Optional.empty();
    }

    String fc3CurAmount;

    public Optional<String> getFC3CurAmount() {
        fc3CurAmount = getValue("FC3_CUR_AMOUNT");
        if (fc3CurAmount != null) {
            int adjAmt = Integer.parseInt(fc3CurAmount)/10000;
            fc3CurAmount = String.valueOf(adjAmt);
        }
        if (fc3CurAmount != null)
            return Optional.of(fc3CurAmount);
        return Optional.empty();
    }

    String fc3ChgAmount;

    public Optional<String> getFC3ChgAmount() {
        fc3ChgAmount = getValue("FC3_CHG_AMOUNT");
        if (fc3ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc3ChgAmount)/10000;
            fc3ChgAmount = String.valueOf(adjAmt);
        }
        if (fc3ChgAmount != null)
            return Optional.of(fc3ChgAmount);
        return Optional.empty();
    }

    String fc4CurAmount;

    public Optional<String> getFC4CurAmount() {
        fc4CurAmount = getValue("FC4_CUR_AMOUNT");
        if (fc4CurAmount != null) {
            int adjAmt = Integer.parseInt(fc4CurAmount)/10000;
            fc4CurAmount = String.valueOf(adjAmt);
        }
        if (fc4CurAmount != null)
            return Optional.of(fc4CurAmount);
        return Optional.empty();
    }

    String fc4ChgAmount;

    public Optional<String> getFC4ChgAmount() {
        fc4ChgAmount = getValue("FC4_CHG_AMOUNT");
        if (fc4ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc4ChgAmount)/10000;
            fc4ChgAmount = String.valueOf(adjAmt);
        }
        if (fc4ChgAmount != null)
            return Optional.of(fc4ChgAmount);
        return Optional.empty();
    }

    String fc5CurAmount;

    public Optional<String> getFC5CurAmount() {
        fc5CurAmount = getValue("FC5_CUR_AMOUNT");
        if (fc5CurAmount != null) {
            int adjAmt = Integer.parseInt(fc5CurAmount)/10000;
            fc5CurAmount = String.valueOf(adjAmt);
        }
        if (fc5CurAmount != null)
            return Optional.of(fc5CurAmount);
        return Optional.empty();
    }

    String fc5ChgAmount;

    public Optional<String> getFC5ChgAmount() {
        fc5ChgAmount = getValue("FC5_CHG_AMOUNT");
        if (fc5ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc5ChgAmount)/10000;
            fc5ChgAmount = String.valueOf(adjAmt);
        }
        if (fc5ChgAmount != null)
            return Optional.of(fc5ChgAmount);
        return Optional.empty();
    }


    String fc6CurAmount;

    public Optional<String> getFC6CurAmount() {
        fc6CurAmount = getValue("FC6_CUR_AMOUNT");
        if (fc6CurAmount != null) {
            int adjAmt = Integer.parseInt(fc6CurAmount)/10000;
            fc6CurAmount = String.valueOf(adjAmt);
        }
        if (fc6CurAmount != null)
            return Optional.of(fc6CurAmount);
        return Optional.empty();
    }

    String fc6ChgAmount;

    public Optional<String> getFC6ChgAmount() {
        fc6ChgAmount = getValue("FC6_CHG_AMOUNT");
        if (fc6ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc6ChgAmount)/10000;
            fc6ChgAmount = String.valueOf(adjAmt);
        }
        if (fc6ChgAmount != null)
            return Optional.of(fc6ChgAmount);
        return Optional.empty();
    }


    String fc7CurAmount;

    public Optional<String> getFC7CurAmount() {
        fc7CurAmount = getValue("FC7_CUR_AMOUNT");
        if (fc7CurAmount != null) {
            int adjAmt = Integer.parseInt(fc7CurAmount)/10000;
            fc7CurAmount = String.valueOf(adjAmt);
        }
        if (fc7CurAmount != null)
            return Optional.of(fc7CurAmount);
        return Optional.empty();
    }

    String fc7ChgAmount;

    public Optional<String> getFC7ChgAmount() {
        fc7ChgAmount = getValue("FC7_CHG_AMOUNT");
        if (fc7ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc7ChgAmount)/10000;
            fc7ChgAmount = String.valueOf(adjAmt);
        }
        if (fc7ChgAmount != null)
            return Optional.of(fc7ChgAmount);
        return Optional.empty();
    }


    String fc8CurAmount;

    public Optional<String> getFC8CurAmount() {
        fc8CurAmount = getValue("FC8_CUR_AMOUNT");
        if (fc8CurAmount != null) {
            int adjAmt = Integer.parseInt(fc8CurAmount)/10000;
            fc8CurAmount = String.valueOf(adjAmt);
        }
        if (fc8CurAmount != null)
            return Optional.of(fc8CurAmount);
        return Optional.empty();
    }

    String fc8ChgAmount;

    public Optional<String> getFC8ChgAmount() {
        fc8ChgAmount = getValue("FC8_CHG_AMOUNT");
        if (fc8ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc8ChgAmount)/10000;
            fc8ChgAmount = String.valueOf(adjAmt);
        }
        if (fc8ChgAmount != null)
            return Optional.of(fc8ChgAmount);
        return Optional.empty();
    }


    String fc9CurAmount;

    public Optional<String> getFC9CurAmount() {
        fc9CurAmount = getValue("FC9_CUR_AMOUNT");
        if (fc9CurAmount != null) {
            int adjAmt = Integer.parseInt(fc9CurAmount)/10000;
            fc9CurAmount = String.valueOf(adjAmt);
        }
        if (fc9CurAmount != null)
            return Optional.of(fc9CurAmount);
        return Optional.empty();
    }

    String fc9ChgAmount;

    public Optional<String> getFC9ChgAmount() {
        fc9ChgAmount = getValue("FC9_CHG_AMOUNT");
        if (fc9ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc9ChgAmount)/10000;
            fc9ChgAmount = String.valueOf(adjAmt);
        }
        if (fc9ChgAmount != null)
            return Optional.of(fc9ChgAmount);
        return Optional.empty();
    }

    String fc10CurAmount;

    public Optional<String> getFC10CurAmount() {
        fc10CurAmount = getValue("FC10_CUR_AMOUNT");
        if (fc10CurAmount != null) {
            int adjAmt = Integer.parseInt(fc10CurAmount)/10000;
            fc10CurAmount = String.valueOf(adjAmt);
        }
        if (fc10CurAmount != null)
            return Optional.of(fc10CurAmount);
        return Optional.empty();
    }

    String fc10ChgAmount;

    public Optional<String> getFC10ChgAmount() {
        fc10ChgAmount = getValue("FC10_CHG_AMOUNT");
        if (fc10ChgAmount != null) {
            int adjAmt = Integer.parseInt(fc10ChgAmount)/10000;
            fc10ChgAmount = String.valueOf(adjAmt);
        }
        if (fc10ChgAmount != null)
            return Optional.of(fc10ChgAmount);
        return Optional.empty();
    }

    String bd1BonusAmt;

    public Optional<String> getBD1BonusAmt() {
        bd1BonusAmt = getValue("BD1_BONUS_AMOUNT");
        if (bd1BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd1BonusAmt)/10000;
            bd1BonusAmt = String.valueOf(adjAmt);
        }
        if (bd1BonusAmt != null)
            return Optional.of(bd1BonusAmt);
        return Optional.empty();
    }

    String bd1CurBalance;

    public Optional<String> getBD1CurBalance() {
        bd1CurBalance = getValue("BD1_CURRENT_BALANCE");
        if (bd1CurBalance != null) {
            int adjAmt = Integer.parseInt(bd1CurBalance)/10000;
            bd1CurBalance = String.valueOf(adjAmt);
        }
        if (bd1CurBalance != null)
            return Optional.of(bd1CurBalance);
        return Optional.empty();
    }

    String bd2BonusAmt;

    public Optional<String> getBD2BonusAmt() {
        bd2BonusAmt = getValue("BD2_BONUS_AMOUNT");
        if (bd2BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd2BonusAmt)/10000;
            bd2BonusAmt = String.valueOf(adjAmt);
        }
        if (bd2BonusAmt != null)
            return Optional.of(bd2BonusAmt);
        return Optional.empty();
    }

    String bd2CurBalance;

    public Optional<String> getBD2CurBalance() {
        bd2CurBalance = getValue("BD2_CURRENT_BALANCE");
        if (bd2CurBalance != null) {
            int adjAmt = Integer.parseInt(bd2CurBalance)/10000;
            bd2CurBalance = String.valueOf(adjAmt);
        }
        if (bd2CurBalance != null)
            return Optional.of(bd2CurBalance);
        return Optional.empty();
    }

    String bd3BonusAmt;

    public Optional<String> getBD3BonusAmt() {
        bd3BonusAmt = getValue("BD3_BONUS_AMOUNT");
        if (bd3BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd3BonusAmt)/10000;
            bd3BonusAmt = String.valueOf(adjAmt);
        }
        if (bd3BonusAmt != null)
            return Optional.of(bd3BonusAmt);
        return Optional.empty();
    }

    String bd3CurBalance;

    public Optional<String> getBD3CurBalance() {
        bd3CurBalance = getValue("BD3_CURRENT_BALANCE");
        if (bd3CurBalance != null) {
            int adjAmt = Integer.parseInt(bd3CurBalance)/10000;
            bd3CurBalance = String.valueOf(adjAmt);
        }
        if (bd3CurBalance != null)
            return Optional.of(bd3CurBalance);
        return Optional.empty();
    }

    String bd4BonusAmt;

    public Optional<String> getBD4BonusAmt() {
        bd4BonusAmt = getValue("BD4_BONUS_AMOUNT");
        if (bd4BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd4BonusAmt)/10000;
            bd4BonusAmt = String.valueOf(adjAmt);
        }
        if (bd4BonusAmt != null)
            return Optional.of(bd4BonusAmt);
        return Optional.empty();
    }

    String bd4CurBalance;

    public Optional<String> getBD4CurBalance() {
        bd4CurBalance = getValue("BD4_CURRENT_BALANCE");
        if (bd4CurBalance != null) {
            int adjAmt = Integer.parseInt(bd4CurBalance)/10000;
            bd4CurBalance = String.valueOf(adjAmt);
        }
        if (bd4CurBalance != null)
            return Optional.of(bd4CurBalance);
        return Optional.empty();
    }

    String bd5BonusAmt;

    public Optional<String> getBD5BonusAmt() {
        bd5BonusAmt = getValue("BD5_BONUS_AMOUNT");
        if (bd5BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd5BonusAmt)/10000;
            bd5BonusAmt = String.valueOf(adjAmt);
        }
        if (bd5BonusAmt != null)
            return Optional.of(bd5BonusAmt);
        return Optional.empty();
    }

    String bd5CurBalance;

    public Optional<String> getBD5CurBalance() {
        bd5CurBalance = getValue("BD5_CURRENT_BALANCE");
        if (bd5CurBalance != null) {
            int adjAmt = Integer.parseInt(bd5CurBalance)/10000;
            bd5CurBalance = String.valueOf(adjAmt);
        }
        if (bd5CurBalance != null)
            return Optional.of(bd5CurBalance);
        return Optional.empty();
    }

    String bd6BonusAmt;

    public Optional<String> getBD6BonusAmt() {
        bd6BonusAmt = getValue("BD6_BONUS_AMOUNT");
        if (bd6BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd6BonusAmt)/10000;
            bd6BonusAmt = String.valueOf(adjAmt);
        }
        if (bd6BonusAmt != null)
            return Optional.of(bd6BonusAmt);
        return Optional.empty();
    }

    String bd6CurBalance;

    public Optional<String> getBD6CurBalance() {
        bd6CurBalance = getValue("BD6_CURRENT_BALANCE");
        if (bd6CurBalance != null) {
            int adjAmt = Integer.parseInt(bd6CurBalance)/10000;
            bd6CurBalance = String.valueOf(adjAmt);
        }
        if (bd6CurBalance != null)
            return Optional.of(bd6CurBalance);
        return Optional.empty();
    }

    String bd7BonusAmt;

    public Optional<String> getBD7BonusAmt() {
        bd7BonusAmt = getValue("BD7_BONUS_AMOUNT");
        if (bd7BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd7BonusAmt)/10000;
            bd7BonusAmt = String.valueOf(adjAmt);
        }
        if (bd7BonusAmt != null)
            return Optional.of(bd7BonusAmt);
        return Optional.empty();
    }

    String bd7CurBalance;

    public Optional<String> getBD7CurBalance() {
        bd7CurBalance = getValue("BD7_CURRENT_BALANCE");
        if (bd7CurBalance != null) {
            int adjAmt = Integer.parseInt(bd7CurBalance)/10000;
            bd7CurBalance = String.valueOf(adjAmt);
        }
        if (bd7CurBalance != null)
            return Optional.of(bd7CurBalance);
        return Optional.empty();
    }

    String bd8BonusAmt;

    public Optional<String> getBD8BonusAmt() {
        bd8BonusAmt = getValue("BD8_BONUS_AMOUNT");
        if (bd8BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd8BonusAmt)/10000;
            bd8BonusAmt = String.valueOf(adjAmt);
        }
        if (bd8BonusAmt != null)
            return Optional.of(bd8BonusAmt);
        return Optional.empty();
    }

    String bd8CurBalance;

    public Optional<String> getBD8CurBalance() {
        bd8CurBalance = getValue("BD8_CURRENT_BALANCE");
        if (bd8CurBalance != null) {
            int adjAmt = Integer.parseInt(bd8CurBalance)/10000;
            bd8CurBalance = String.valueOf(adjAmt);
        }
        if (bd8CurBalance != null)
            return Optional.of(bd8CurBalance);
        return Optional.empty();
    }

    String bd9BonusAmt;

    public Optional<String> getBD9BonusAmt() {
        bd9BonusAmt = getValue("BD9_BONUS_AMOUNT");
        if (bd9BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd9BonusAmt)/10000;
            bd9BonusAmt = String.valueOf(adjAmt);
        }
        if (bd9BonusAmt != null)
            return Optional.of(bd9BonusAmt);
        return Optional.empty();
    }

    String bd9CurBalance;

    public Optional<String> getBD9CurBalance() {
        bd9CurBalance = getValue("BD9_CURRENT_BALANCE");
        if (bd9CurBalance != null) {
            int adjAmt = Integer.parseInt(bd9CurBalance)/10000;
            bd9CurBalance = String.valueOf(adjAmt);
        }
        if (bd9CurBalance != null)
            return Optional.of(bd9CurBalance);
        return Optional.empty();
    }

    String bd10BonusAmt;

    public Optional<String> getBD10BonusAmt() {
        bd10BonusAmt = getValue("BD10_BONUS_AMOUNT");
        if (bd10BonusAmt != null) {
            int adjAmt = Integer.parseInt(bd10BonusAmt)/10000;
            bd10BonusAmt = String.valueOf(adjAmt);
        }
        if (bd10BonusAmt != null)
            return Optional.of(bd10BonusAmt);
        return Optional.empty();
    }

    String bd10CurBalance;

    public Optional<String> getBD10CurBalance() {
        bd10CurBalance = getValue("BD10_CURRENT_BALANCE");
        if (bd10CurBalance != null) {
            int adjAmt = Integer.parseInt(bd10CurBalance)/10000;
            bd10CurBalance = String.valueOf(adjAmt);
        }
        if (bd10CurBalance != null)
            return Optional.of(bd10CurBalance);
        return Optional.empty();
    }

    String fr1BonusAmt;

    public Optional<String> getFR1BonusAmt() {
        fr1BonusAmt = getValue("FR1_BONUS_AMOUNT");
        if (fr1BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr1BonusAmt)/10000;
            fr1BonusAmt = String.valueOf(adjAmt);
        }
        if (fr1BonusAmt != null)
            return Optional.of(fr1BonusAmt);
        return Optional.empty();
    }

    String fr1CurAmt;

    public Optional<String> getFR1CurBalance() {
        fr1CurAmt = getValue("FR1_CURRENT_AMOUNT");
        if (fr1CurAmt != null) {
            int adjAmt = Integer.parseInt(fr1CurAmt)/10000;
            fr1CurAmt = String.valueOf(adjAmt);
        }
        if (fr1CurAmt != null)
            return Optional.of(fr1CurAmt);
        return Optional.empty();
    }

    String fr2BonusAmt;

    public Optional<String> getFR2BonusAmt() {
        fr2BonusAmt = getValue("FR2_BONUS_AMOUNT");
        if (fr2BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr2BonusAmt)/10000;
            fr2BonusAmt = String.valueOf(adjAmt);
        }
        if (fr2BonusAmt != null)
            return Optional.of(fr2BonusAmt);
        return Optional.empty();
    }

    String fr2CurAmt;

    public Optional<String> getFR2CurBalance() {
        fr2CurAmt = getValue("FR2_CURRENT_AMOUNT");
        if (fr2CurAmt != null) {
            int adjAmt = Integer.parseInt(fr2CurAmt)/10000;
            fr2CurAmt = String.valueOf(adjAmt);
        }
        if (fr2CurAmt != null)
            return Optional.of(fr2CurAmt);
        return Optional.empty();
    }

    String fr3BonusAmt;

    public Optional<String> getFR3BonusAmt() {
        fr3BonusAmt = getValue("FR3_BONUS_AMOUNT");
        if (fr3BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr3BonusAmt)/10000;
            fr3BonusAmt = String.valueOf(adjAmt);
        }
        if (fr3BonusAmt != null)
            return Optional.of(fr3BonusAmt);
        return Optional.empty();
    }

    String fr3CurAmt;

    public Optional<String> getFR3CurBalance() {
        fr3CurAmt = getValue("FR3_CURRENT_AMOUNT");
        if (fr3CurAmt != null) {
            int adjAmt = Integer.parseInt(fr3CurAmt)/10000;
            fr3CurAmt = String.valueOf(adjAmt);
        }
        if (fr3CurAmt != null)
            return Optional.of(fr3CurAmt);
        return Optional.empty();
    }

    String fr4BonusAmt;

    public Optional<String> getFR4BonusAmt() {
        fr4BonusAmt = getValue("FR4_BONUS_AMOUNT");
        if (fr4BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr4BonusAmt)/10000;
            fr4BonusAmt = String.valueOf(adjAmt);
        }
        if (fr4BonusAmt != null)
            return Optional.of(fr4BonusAmt);
        return Optional.empty();
    }

    String fr4CurAmt;

    public Optional<String> getFR4CurBalance() {
        fr4CurAmt = getValue("FR4_CURRENT_AMOUNT");
        if (fr4CurAmt != null) {
            int adjAmt = Integer.parseInt(fr4CurAmt)/10000;
            fr4CurAmt = String.valueOf(adjAmt);
        }
        if (fr4CurAmt != null)
            return Optional.of(fr4CurAmt);
        return Optional.empty();
    }

    String fr5BonusAmt;

    public Optional<String> getFR5BonusAmt() {
        fr5BonusAmt = getValue("FR5_BONUS_AMOUNT");
        if (fr5BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr5BonusAmt)/10000;
            fr5BonusAmt = String.valueOf(adjAmt);
        }
        if (fr5BonusAmt != null)
            return Optional.of(fr5BonusAmt);
        return Optional.empty();
    }

    String fr5CurAmt;

    public Optional<String> getFR5CurBalance() {
        fr5CurAmt = getValue("FR5_CURRENT_AMOUNT");
        if (fr5CurAmt != null) {
            int adjAmt = Integer.parseInt(fr5CurAmt)/10000;
            fr5CurAmt = String.valueOf(adjAmt);
        }
        if (fr5CurAmt != null)
            return Optional.of(fr5CurAmt);
        return Optional.empty();
    }

    String fr6BonusAmt;

    public Optional<String> getFR6BonusAmt() {
        fr6BonusAmt = getValue("FR6_BONUS_AMOUNT");
        if (fr6BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr6BonusAmt)/10000;
            fr6BonusAmt = String.valueOf(adjAmt);
        }
        if (fr6BonusAmt != null)
            return Optional.of(fr6BonusAmt);
        return Optional.empty();
    }

    String fr6CurAmt;

    public Optional<String> getFR6CurBalance() {
        fr6CurAmt = getValue("FR6_CURRENT_AMOUNT");
        if (fr6CurAmt != null) {
            int adjAmt = Integer.parseInt(fr6CurAmt)/10000;
            fr6CurAmt = String.valueOf(adjAmt);
        }
        if (fr6CurAmt != null)
            return Optional.of(fr6CurAmt);
        return Optional.empty();
    }

    String fr7BonusAmt;

    public Optional<String> getFR7BonusAmt() {
        fr7BonusAmt = getValue("FR7_BONUS_AMOUNT");
        if (fr7BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr7BonusAmt)/10000;
            fr7BonusAmt = String.valueOf(adjAmt);
        }
        if (fr7BonusAmt != null)
            return Optional.of(fr7BonusAmt);
        return Optional.empty();
    }

    String fr7CurAmt;

    public Optional<String> getFR7CurBalance() {
        fr7CurAmt = getValue("FR7_CURRENT_AMOUNT");
        if (fr7CurAmt != null) {
            int adjAmt = Integer.parseInt(fr7CurAmt)/10000;
            fr7CurAmt = String.valueOf(adjAmt);
        }
        if (fr7CurAmt != null)
            return Optional.of(fr7CurAmt);
        return Optional.empty();
    }

    String fr8BonusAmt;

    public Optional<String> getFR8BonusAmt() {
        fr8BonusAmt = getValue("FR8_BONUS_AMOUNT");
        if (fr8BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr8BonusAmt)/10000;
            fr8BonusAmt = String.valueOf(adjAmt);
        }
        if (fr8BonusAmt != null)
            return Optional.of(fr8BonusAmt);
        return Optional.empty();
    }

    String fr8CurAmt;

    public Optional<String> getFR8CurBalance() {
        fr8CurAmt = getValue("FR8_CURRENT_AMOUNT");
        if (fr8CurAmt != null) {
            int adjAmt = Integer.parseInt(fr8CurAmt)/10000;
            fr8CurAmt = String.valueOf(adjAmt);
        }
        if (fr8CurAmt != null)
            return Optional.of(fr8CurAmt);
        return Optional.empty();
    }

    String fr9BonusAmt;

    public Optional<String> getFR9BonusAmt() {
        fr9BonusAmt = getValue("FR9_BONUS_AMOUNT");
        if (fr9BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr9BonusAmt)/10000;
            fr9BonusAmt = String.valueOf(adjAmt);
        }
        if (fr9BonusAmt != null)
            return Optional.of(fr9BonusAmt);
        return Optional.empty();
    }

    String fr9CurAmt;

    public Optional<String> getFR9CurBalance() {
        fr9CurAmt = getValue("FR9_CURRENT_AMOUNT");
        if (fr9CurAmt != null) {
            int adjAmt = Integer.parseInt(fr9CurAmt)/10000;
            fr9CurAmt = String.valueOf(adjAmt);
        }
        if (fr9CurAmt != null)
            return Optional.of(fr9CurAmt);
        return Optional.empty();
    }

    String fr10BonusAmt;

    public Optional<String> getFR10BonusAmt() {
        fr10BonusAmt = getValue("FR10_BONUS_AMOUNT");
        if (fr10BonusAmt != null) {
            int adjAmt = Integer.parseInt(fr10BonusAmt)/10000;
            fr10BonusAmt = String.valueOf(adjAmt);
        }
        if (fr10BonusAmt != null)
            return Optional.of(fr10BonusAmt);
        return Optional.empty();
    }

    String fr10CurAmt;

    public Optional<String> getFR10CurBalance() {
        fr10CurAmt = getValue("FR10_CURRENT_AMOUNT");
        if (fr10CurAmt != null) {
            int adjAmt = Integer.parseInt(fr10CurAmt)/10000;
            fr10CurAmt = String.valueOf(adjAmt);
        }
        if (fr10CurAmt != null)
            return Optional.of(fr10CurAmt);
        return Optional.empty();
    }

    String loanAmt;

    public Optional<String> getLoanAmt() {
        loanAmt = getValue("LOAN_AMOUNT");
        if (loanAmt != null) {
            int adjAmt = Integer.parseInt(loanAmt)/10000;
            loanAmt = String.valueOf(adjAmt);
        }
        if (loanAmt != null)
            return Optional.of(loanAmt);
        return Optional.empty();
    }

    String loanPoundage;

    public Optional<String> getLoanPoundage() {
        loanPoundage = getValue("LOAN_POUNDAGE");
        if (loanPoundage != null) {
            int adjAmt = Integer.parseInt(loanPoundage)/10000;
            loanPoundage = String.valueOf(adjAmt);
        }
        if (loanPoundage != null)
            return Optional.of(loanPoundage);
        return Optional.empty();
    }

    String loanPenalty;

    public Optional<String> getLoanPenalty() {
        loanPenalty = getValue("LOAN_PENALTY");
        if (loanPenalty != null) {
            int adjAmt = Integer.parseInt(loanPenalty)/10000;
            loanPenalty = String.valueOf(adjAmt);
        }
        if (loanPenalty != null)
            return Optional.of(loanPenalty);
        return Optional.empty();
    }

    String taxAmt;

    public Optional<String> getTaxAmt() {
        taxAmt = getValue("TAX_AMOUNT");
        if (taxAmt != null) {
            int adjAmt = Integer.parseInt(taxAmt)/10000;
            taxAmt = String.valueOf(adjAmt);
        }
        if (taxAmt != null)
            return Optional.of(taxAmt);
        return Optional.empty();
    }

    String BC1OperType;

    public Optional<String> getBC1OperType() {
        BC1OperType = getValue("BC1_OPER_TYPE");
        if (BC1OperType != null) {
            switch (BC1OperType) {
                case "0":
                    BC1OperType = "expend";
                    break;
                case "1":
                    BC1OperType = "perform accounting";
                    break;
                case "2":
                    BC1OperType = "update the income";
                    break;
                case "3":
                    BC1OperType = "increase the income";
                    break;
                default:
                    BC1OperType = "unknown";
                    break;
            }
        }
        if (BC1OperType != null)
            return Optional.of(BC1OperType);
        return Optional.empty();
    }

    String FC1FuOwnType;

    public Optional<String> getFC1FUOwnType() {
        FC1FuOwnType = getValue("FC1_FU_OWN_TYPE");
        if (FC1FuOwnType != null) {
            switch (FC1FuOwnType.toUpperCase()) {
                case "S":
                    FC1FuOwnType = "subscriber";
                    break;
                case "G":
                    FC1FuOwnType = "subscriber group";
                    break;
                case "C":
                    FC1FuOwnType = "customer";
                    break;
                default:
                    FC1FuOwnType = "unknown";
                    break;
            }
        }
        if (FC1FuOwnType != null)
            return Optional.of(FC1FuOwnType);
        return Optional.empty();
    }

    String FC1OperType;

    public Optional<String> getFC1OperType() {
        FC1OperType = getValue("FC1_OPER_TYPE");
        if (FC1OperType != null) {
            switch (FC1OperType) {
                case "0":
                    FC1OperType = "expend";
                    break;
                case "1":
                    FC1OperType = "update the income";
                    break;
                case "2":
                    FC1OperType = "increase the income";
                    break;
                default:
                    FC1OperType = "unknown";
                    break;
            }
        }
        if (FC1OperType != null)
            return Optional.of(FC1OperType);
        return Optional.empty();
    }

    String BD1OperType;

    public Optional<String> getBD1OperType() {
        BD1OperType = getValue("BD1_OPER_TYPE");
        if (BD1OperType != null) {
            switch (BD1OperType) {
                case "4":
                    BD1OperType = "roll over";
                    break;
                case "5":
                    BD1OperType = "reward";
                    break;
                case "6":
                    BD1OperType = "update the reward";
                    break;
                default:
                    BD1OperType = "unknown";
                    break;
            }
        }
        if (BD1OperType != null)
            return Optional.of(BD1OperType);
        return Optional.empty();
    }

    String FR1FuOwnType;

    public Optional<String> getFR1FUOwnType() {
        FR1FuOwnType = getValue("FR1_FU_OWN_TYPE");
        if (FR1FuOwnType != null) {
            switch (FR1FuOwnType) {
                case "S":
                    FR1FuOwnType = "subscriber";
                    break;
                case "G":
                    FR1FuOwnType = "subscriber group";
                    break;
                default:
                    FR1FuOwnType = "unknown";
                    break;
            }
        }
        if (FR1FuOwnType != null)
            return Optional.of(FR1FuOwnType);
        return Optional.empty();
    }

    String FR1OperType;

    public Optional<String> getFR1OperType() {
        FR1OperType = getValue("FR1_OPER_TYPE");
        if (FR1OperType != null) {
            switch (FR1OperType) {
                case "4":
                    FR1OperType = "roll over";
                    break;
                case "5":
                    FR1OperType = "reward";
                    break;
                case "6":
                    FR1OperType = "update the reward";
                    break;
                default:
                    FR1OperType = "unknown";
                    break;
            }
        }
        if (FR1OperType != null)
            return Optional.of(FR1OperType);
        return Optional.empty();
    }

    String oldStatus;

    public Optional<String> getOldStatus() {
        oldStatus = getValue("OLD_STATUS");
        if (oldStatus != null) {
            switch (oldStatus) {
                case "1":
                    oldStatus = "Idle";
                    break;
                case "2":
                    oldStatus = "Active";
                    break;
                case "3":
                case "4":
                case "5":
                case "6":
                case "7":
                case "8":
                case "9":
                    oldStatus = "To be extended";
                    break;
                default:
                    oldStatus = "unknown";
                    break;
            }
        }
        if (oldStatus != null)
            return Optional.of(oldStatus);
        return Optional.empty();
    }

    String currentStatus;

    public Optional<String> getCurrentStatus() {
        currentStatus = getValue("CURRENT_STATUS");
        if (currentStatus != null) {
            switch (currentStatus) {
                case "1":
                    currentStatus = "Idle";
                    break;
                case "2":
                    currentStatus = "Active";
                    break;
                case "3":
                case "4":
                case "5":
                case "6":
                case "7":
                case "8":
                case "9":
                    currentStatus = "To be extended";
                    break;
                default:
                    currentStatus = "unknown";
                    break;
            }
        }
        if (currentStatus != null)
            return Optional.of(currentStatus);
        return Optional.empty();
    }

    String payType;

    public Optional<String> getServedType() {
        payType = getValue("PayType");

        if (payType != null) {
            switch (payType) {
                case "0":
                    payType = "PREPAID";
                    break;
                case "1":
                    payType = "POSTPAID";
                    break;
                case "2":
                    payType = "HYBRID";
                    break;
                default:
                    payType = "UNKNOWN";
                    break;
            }
        }
        if (payType != null)
            return Optional.of(payType);
        return Optional.empty();
    }
}
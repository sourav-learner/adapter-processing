package com.gamma.skybase.build.server.etl.decoder.cbs_voice;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class CbsVoiceEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();

    private CbsVoiceEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsVoiceEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsVoiceEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String status;

    public Optional<String> getStatus() {
        status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "A":
                    status = "VALID";
                    break;
                case "D":
                    status = "INVALID";
                    break;
                default:
                    status = "UNKNOWN";
                    break;
            }
        }
        if (status != null)
            return Optional.of(status);
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

    public Optional<String> getEndTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callEndTime = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(callEndTime));
            }
        } catch (Exception e) {
        }
        return Optional.empty();
    }

    String objType;

    public Optional<String> getObjType() {
        objType = getValue("OBJ_TYPE");
        if (objType != null) {
            switch (objType) {
                case "A":
                    objType = "ACC";
                    break;
                case "C":
                    objType = "CUSTOMER";
                    break;
                case "S":
                    objType = "SUBSCRIBER";
                    break;
                case "G":
                    objType = "GROUP";
                    break;
                default:
                    objType = "UNKNOWN";
                    break;
            }
        }
        if (objType != null)
            return Optional.of(objType);
        return Optional.empty();
    }

    String eventTypeKey;

    public Optional<String> getEventTypeKey() {
        serviceFlow = getValue("ServiceFlow");
        if (serviceFlow != null) {
            switch (serviceFlow) {
                case "3":
                    eventTypeKey = "9";
                    break;
                default:
                    eventTypeKey = serviceFlow;
                    break;
            }
        }
        if (eventTypeKey != null)
            return Optional.of(eventTypeKey);
        return Optional.empty();
    }

    String resultCode;

    public Optional<String> getResultCode() {
        resultCode = getValue("RESULT_CODE");
        if (resultCode != null) {
            switch (resultCode) {
                case "0":
                    resultCode = "Success";
                    break;
                default:
                    resultCode = "Fail";
                    break;
            }
        }
        if (resultCode != null)
            return Optional.of(resultCode);
        return Optional.empty();
    }

    String actualUsage, freeUnitAMountOfDuration, rateUsage, bc1BalanceType, bc2BalanceType, bc3BalanceType, bc4BalanceType, bc5BalanceType, bc6BalanceType, bc7BalanceType, bc8BalanceType, bc9BalanceType, bc10BalanceType;
    String bc1ChgeBalance, bc2ChgeBalance, bc3ChgeBalance, bc4ChgeBalance, bc5ChgeBalance, bc6ChgeBalance, bc7ChgeBalance, bc8ChgeBalance, bc9ChgeBalance, bc10ChgeBalance, debitAmount;

    Double result, result1, actualUsagePayg, amount;
    String bc1Chg, bc2Chg, bc3Chg, bc4Chg, bc5Chg, bc6Chg, bc7Chg, bc8Chg, bc9Chg, bc10Chg;

    public Optional<Double> getActualUsagePayg() {
        actualUsage = getValue("ACTUAL_USAGE");
        freeUnitAMountOfDuration = getValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUsage = getValue("RATE_USAGE");
        bc1BalanceType = getValue("BC1_BALANCE_TYPE");
        bc2BalanceType = getValue("BC2_BALANCE_TYPE");
        bc3BalanceType = getValue("BC3_BALANCE_TYPE");
        bc4BalanceType = getValue("BC4_BALANCE_TYPE");
        bc5BalanceType = getValue("BC5_BALANCE_TYPE");
        bc6BalanceType = getValue("BC6_BALANCE_TYPE");
        bc7BalanceType = getValue("BC7_BALANCE_TYPE");
        bc8BalanceType = getValue("BC8_BALANCE_TYPE");
        bc9BalanceType = getValue("BC9_BALANCE_TYPE");
        bc10BalanceType = getValue("BC10_BALANCE_TYPE");
        bc1ChgeBalance = getValue("BC1_CHG_BALANCE");
        bc2ChgeBalance = getValue("BC2_CHG_BALANCE");
        bc3ChgeBalance = getValue("BC3_CHG_BALANCE");
        bc4ChgeBalance = getValue("BC4_CHG_BALANCE");
        bc5ChgeBalance = getValue("BC5_CHG_BALANCE");
        bc6ChgeBalance = getValue("BC6_CHG_BALANCE");
        bc7ChgeBalance = getValue("BC7_CHG_BALANCE");
        bc8ChgeBalance = getValue("BC8_CHG_BALANCE");
        bc9ChgeBalance = getValue("BC9_CHG_BALANCE");
        bc10ChgeBalance = getValue("BC10_CHG_BALANCE");
        debitAmount = getValue("DEBIT_AMOUNT");

        if (freeUnitAMountOfDuration != null && actualUsage != null && rateUsage != null) {
            amount = (Double.parseDouble(freeUnitAMountOfDuration) * Double.parseDouble(actualUsage)) / Double.parseDouble(rateUsage);
            result = Double.parseDouble(actualUsage) - amount;
        }
        if (bc1BalanceType != null) {
            if (bc1BalanceType.equalsIgnoreCase("2000") || bc1BalanceType.equalsIgnoreCase("3000") || bc1BalanceType.equalsIgnoreCase("7000")) {
                if (bc1ChgeBalance != null) {
                    bc1Chg = bc1ChgeBalance;
                } else {
                    bc1Chg = "0";
                }
            } else {
                bc1Chg = "0";
            }
        }

        if (bc2BalanceType != null) {
            if (bc2BalanceType.equalsIgnoreCase("2000") || bc2BalanceType.equalsIgnoreCase("3000") || bc2BalanceType.equalsIgnoreCase("7000")) {
                if (bc2ChgeBalance != null) {
                    bc2Chg = bc2ChgeBalance;
                } else {
                    bc2Chg = "0";
                }
            } else {
                bc2Chg = "0";
            }
        }

        if (bc3BalanceType != null) {
            if (bc3BalanceType.equalsIgnoreCase("2000") || bc3BalanceType.equalsIgnoreCase("3000") || bc3BalanceType.equalsIgnoreCase("7000")) {
                if (bc3ChgeBalance != null) {
                    bc3Chg = bc3ChgeBalance;
                } else {
                    bc3Chg = "0";
                }
            } else {
                bc3Chg = "0";
            }
        }
        if (bc4BalanceType != null) {
            if (bc4BalanceType.equalsIgnoreCase("2000") || bc4BalanceType.equalsIgnoreCase("3000") || bc4BalanceType.equalsIgnoreCase("7000")) {
                if (bc4ChgeBalance != null) {
                    bc4Chg = bc4ChgeBalance;
                } else {
                    bc4Chg = "0";
                }
            } else {
                bc4Chg = "0";
            }
        }
        if (bc5BalanceType != null) {
            if (bc5BalanceType.equalsIgnoreCase("2000") || bc5BalanceType.equalsIgnoreCase("3000") || bc5BalanceType.equalsIgnoreCase("7000")) {
                if (bc5ChgeBalance != null) {
                    bc5Chg = bc5ChgeBalance;
                } else {
                    bc5Chg = "0";
                }
            } else {
                bc5Chg = "0";
            }
        }
        if (bc6BalanceType != null) {
            if (bc6BalanceType.equalsIgnoreCase("2000") || bc6BalanceType.equalsIgnoreCase("3000") || bc6BalanceType.equalsIgnoreCase("7000")) {
                if (bc6ChgeBalance != null) {
                    bc6Chg = bc6ChgeBalance;
                } else {
                    bc6Chg = "0";
                }
            } else {
                bc6Chg = "0";
            }
        }
        if (bc7BalanceType != null) {
            if (bc7BalanceType.equalsIgnoreCase("2000") || bc7BalanceType.equalsIgnoreCase("3000") || bc7BalanceType.equalsIgnoreCase("7000")) {
                if (bc7ChgeBalance != null) {
                    bc7Chg = bc7ChgeBalance;
                } else {
                    bc7Chg = "0";
                }
            } else {
                bc7Chg = "0";
            }
        }
        if (bc8BalanceType != null) {
            if (bc8BalanceType.equalsIgnoreCase("2000") || bc8BalanceType.equalsIgnoreCase("3000") || bc8BalanceType.equalsIgnoreCase("7000")) {
                if (bc8ChgeBalance != null) {
                    bc8Chg = bc8ChgeBalance;
                } else {
                    bc8Chg = "0";
                }
            } else {
                bc8Chg = "0";
            }
        }
        if (bc9BalanceType != null) {
            if (bc9BalanceType.equalsIgnoreCase("2000") || bc9BalanceType.equalsIgnoreCase("3000") || bc9BalanceType.equalsIgnoreCase("7000")) {
                if (bc9ChgeBalance != null) {
                    bc9Chg = bc9ChgeBalance;
                } else {
                    bc9Chg = "0";
                }
            } else {
                bc9Chg = "0";
            }
        }
        if (bc10BalanceType != null) {
            if (bc10BalanceType.equalsIgnoreCase("2000") || bc10BalanceType.equalsIgnoreCase("3000") || bc10BalanceType.equalsIgnoreCase("7000")) {
                if (bc10ChgeBalance != null) {
                    bc10Chg = bc10ChgeBalance;
                } else {
                    bc10Chg = "0";
                }
            } else {
                bc10Chg = "0";
            }
        }
        if (result != null && bc1Chg != null && bc2Chg != null && bc3Chg != null && bc4Chg != null && bc5Chg != null && bc6Chg != null && bc7Chg != null && bc8Chg != null && bc9Chg != null && bc10Chg != null){
            result1 = (result * (Integer.parseInt(bc1Chg)) + (Integer.parseInt(bc2Chg)) + (Integer.parseInt(bc3Chg)) + (Integer.parseInt(bc4Chg)) + (Integer.parseInt(bc5Chg)) + (Integer.parseInt(bc6Chg)) + (Integer.parseInt(bc7Chg)) + (Integer.parseInt(bc8Chg)) + (Integer.parseInt(bc9Chg)) + (Integer.parseInt(bc10Chg)));
        }

        if(debitAmount != null && result1 != null){
            actualUsagePayg = result1 / Double.parseDouble(debitAmount);
        }

        if (actualUsagePayg != null)
            return Optional.of(actualUsagePayg);
        return Optional.empty();
    }

    Double result2, result3, actualUsageBonus, amount1;
    String actualUsg, freeUnitAmount, rateUsg, bc1ChgBal, bc2ChgBal, bc3ChgBal, bc4ChgBal, bc5ChgBal, bc6ChgBal, bc7ChgBal, bc8ChgBal, bc9ChgBal, bc10ChgBal;
    String bc1Bal, bc2Bal, bc3Bal, bc4Bal, bc5Bal, bc6Bal, bc7Bal, bc8Bal, bc9Bal, bc10Bal, debitAmounts;
    String bc1b, bc2b, bc3b, bc4b, bc5b, bc6b, bc7b, bc8b, bc9b, bc10b;

    public Optional<Double> getActualUsageBonus() {
        actualUsg = getValue("ACTUAL_USAGE");
        freeUnitAmount = getValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUsg = getValue("RATE_USAGE");
        bc1Bal = getValue("BC1_BALANCE_TYPE");
        bc2Bal = getValue("BC2_BALANCE_TYPE");
        bc3Bal = getValue("BC3_BALANCE_TYPE");
        bc4Bal = getValue("BC4_BALANCE_TYPE");
        bc5Bal = getValue("BC5_BALANCE_TYPE");
        bc6Bal = getValue("BC6_BALANCE_TYPE");
        bc7Bal = getValue("BC7_BALANCE_TYPE");
        bc8Bal = getValue("BC8_BALANCE_TYPE");
        bc9Bal = getValue("BC9_BALANCE_TYPE");
        bc10Bal = getValue("BC10_BALANCE_TYPE");
        bc1ChgBal = getValue("BC1_CHG_BALANCE");
        bc2ChgBal = getValue("BC2_CHG_BALANCE");
        bc3ChgBal = getValue("BC3_CHG_BALANCE");
        bc4ChgBal = getValue("BC4_CHG_BALANCE");
        bc5ChgBal = getValue("BC5_CHG_BALANCE");
        bc6ChgBal = getValue("BC6_CHG_BALANCE");
        bc7ChgBal = getValue("BC7_CHG_BALANCE");
        bc8ChgBal = getValue("BC8_CHG_BALANCE");
        bc9ChgBal = getValue("BC9_CHG_BALANCE");
        bc10ChgBal = getValue("BC10_CHG_BALANCE");
        debitAmounts = getValue("DEBIT_AMOUNT");

        if (actualUsg != null && freeUnitAmount != null && rateUsg != null) {
            amount1 = (Double.parseDouble(freeUnitAmount) * Double.parseDouble(actualUsg)) / Double.parseDouble(rateUsg);
            result2 = Double.parseDouble(actualUsg) - amount1;
        }
        if (bc1Bal != null) {
            if ((!bc1Bal.equalsIgnoreCase("2000")) || (!bc1Bal.equalsIgnoreCase("3000")) || (!bc1Bal.equalsIgnoreCase("7000"))) {
                if (bc1ChgBal != null) {
                    bc1b = bc1ChgBal;
                } else {
                    bc1b = "0";
                }
            } else {
                bc1b = "0";
            }
        }
        if (bc2Bal != null) {
            if ((!bc2Bal.equalsIgnoreCase("2000")) || (!bc2Bal.equalsIgnoreCase("3000")) || (!bc2Bal.equalsIgnoreCase("7000"))) {
                if (bc2ChgBal != null) {
                    bc2b = bc2ChgBal;
                } else {
                    bc2b = "0";
                }
            } else {
                bc2b = "0";
            }
        }
        if (bc3Bal != null) {
            if ((!bc3Bal.equalsIgnoreCase("2000")) || (!bc3Bal.equalsIgnoreCase("3000")) || (!bc3Bal.equalsIgnoreCase("7000"))) {
                if (bc3ChgBal != null) {
                    bc3b = bc3ChgBal;
                } else {
                    bc3b = "0";
                }
            } else {
                bc3b = "0";
            }
        }
        if (bc4Bal != null) {
            if ((!bc4Bal.equalsIgnoreCase("2000")) || (!bc4Bal.equalsIgnoreCase("3000")) || (!bc4Bal.equalsIgnoreCase("7000"))) {
                if (bc4ChgBal != null) {
                    bc4b = bc4ChgBal;
                } else {
                    bc4b = "0";
                }
            } else {
                bc4b = "0";
            }
        }
        if (bc5Bal != null) {
            if ((!bc5Bal.equalsIgnoreCase("2000")) || (!bc5Bal.equalsIgnoreCase("3000")) || (!bc5Bal.equalsIgnoreCase("7000"))) {
                if (bc5ChgBal != null) {
                    bc5b = bc5ChgBal;
                } else {
                    bc5b = "0";
                }
            } else {
                bc5b = "0";
            }
        }
        if (bc6Bal != null) {
            if ((!bc6Bal.equalsIgnoreCase("2000")) || (!bc6Bal.equalsIgnoreCase("3000")) || (!bc6Bal.equalsIgnoreCase("7000"))) {
                if (bc6ChgBal != null) {
                    bc6b = bc6ChgBal;
                } else {
                    bc6b = "0";
                }
            } else {
                bc6b = "0";
            }
        }
        if (bc7Bal != null) {
            if ((!bc7Bal.equalsIgnoreCase("2000")) || (!bc7Bal.equalsIgnoreCase("3000")) || (!bc7Bal.equalsIgnoreCase("7000"))) {
                if (bc7ChgBal != null) {
                    bc7b = bc7ChgBal;
                } else {
                    bc7b = "0";
                }
            } else {
                bc7b = "0";
            }
        }
        if (bc8Bal != null) {
            if ((!bc8Bal.equalsIgnoreCase("2000")) || (!bc8Bal.equalsIgnoreCase("3000")) || (!bc8Bal.equalsIgnoreCase("7000"))) {
                if (bc8ChgBal != null) {
                    bc8b = bc8ChgBal;
                } else {
                    bc8b = "0";
                }
            } else {
                bc8b = "0";
            }
        }
        if (bc9Bal != null) {
            if ((!bc9Bal.equalsIgnoreCase("2000")) || (!bc9Bal.equalsIgnoreCase("3000")) || (!bc9Bal.equalsIgnoreCase("7000"))) {
                if (bc9ChgBal != null) {
                    bc9b = bc9ChgBal;
                } else {
                    bc9b = "0";
                }
            } else {
                bc9b = "0";
            }
        }
        if (bc10Bal != null) {
            if ((!bc10Bal.equalsIgnoreCase("2000")) || (!bc10Bal.equalsIgnoreCase("3000")) || (!bc10Bal.equalsIgnoreCase("7000"))) {
                if (bc10ChgBal != null) {
                    bc10b = bc10ChgBal;
                } else {
                    bc10b = "0";
                }
            } else {
                bc10b = "0";
            }
        }

        if (result2 != null && bc1b != null && bc2b !=null && bc3b !=null && bc4b != null && bc5b != null && bc6b!= null && bc7b != null && bc8b != null && bc9b != null && bc10b != null){
            result3 = (result2 * (Integer.parseInt(bc1b)) + (Integer.parseInt(bc2b)) + (Integer.parseInt(bc3b)) + (Integer.parseInt(bc4b)) + (Integer.parseInt(bc5b)) + (Integer.parseInt(bc6b)) + (Integer.parseInt(bc7b)) + (Integer.parseInt(bc8b)) + (Integer.parseInt(bc9b)) + (Integer.parseInt(bc10b)));
        }

        if (debitAmounts != null && result3 != null){
            actualUsageBonus = result3 / Double.parseDouble(debitAmounts);
        }

        if (actualUsageBonus != null)
            return Optional.of(actualUsageBonus);
        return Optional.empty();
    }

    String freeUnitAmountOfDuration1, actualUsage1, rateUsage1;
    Double allowance, allowance1;

    public Optional<Double> getActualUsageAllowance() {
        rateUsage1 = getValue("RATE_USAGE");
        freeUnitAmountOfDuration1 = getValue("FREE_UNIT_AMOUNT_OF_DURATION");
        actualUsage1 = getValue("ACTUAL_USAGE");

        if (freeUnitAmountOfDuration1 != null && actualUsage1 != null && rateUsage1 != null) {
            allowance1 = (Double.parseDouble(freeUnitAmountOfDuration1)) * (Double.parseDouble(actualUsage1));
            allowance = allowance1 / (Double.parseDouble(rateUsage1));
        }
        if (Double.isNaN(allowance)){
            return Optional.empty();
        }
        else {
            return Optional.of(allowance);
        }
    }

    String rateUse, freeUnit, bc1Balance, bc2Balance, bc3Balance, bc4Balance, bc5Balance, bc6Balance, bc7Balance, bc8Balance, bc9Balance, bc10Balance;
    String bc1ChgBalance, bc2ChgBalance, bc3ChgBalance, bc4ChgBalance, bc5ChgBalance, bc6ChgBalance, bc7ChgBalance, bc8ChgBalance, bc9ChgBalance, bc10ChgBalance, debits;
    Double results, rateUsagePayg, amounts;
    String bc1c, bc2c, bc3c, bc4c, bc5c, bc6c, bc7c, bc8c, bc9c, bc10c;

    public Optional<Double> getRateUsagePayg() {
        rateUse = getValue("RATE_USAGE");
        freeUnit = getValue("FREE_UNIT_AMOUNT_OF_DURATION");
        bc1Balance = getValue("BC1_BALANCE_TYPE");
        bc2Balance = getValue("BC2_BALANCE_TYPE");
        bc3Balance = getValue("BC3_BALANCE_TYPE");
        bc4Balance = getValue("BC4_BALANCE_TYPE");
        bc5Balance = getValue("BC5_BALANCE_TYPE");
        bc6Balance = getValue("BC6_BALANCE_TYPE");
        bc7Balance = getValue("BC7_BALANCE_TYPE");
        bc8Balance = getValue("BC8_BALANCE_TYPE");
        bc9Balance = getValue("BC9_BALANCE_TYPE");
        bc10Balance = getValue("BC10_BALANCE_TYPE");
        bc1ChgBalance = getValue("BC1_CHG_BALANCE");
        bc2ChgBalance = getValue("BC2_CHG_BALANCE");
        bc3ChgBalance = getValue("BC3_CHG_BALANCE");
        bc4ChgBalance = getValue("BC4_CHG_BALANCE");
        bc5ChgBalance = getValue("BC5_CHG_BALANCE");
        bc6ChgBalance = getValue("BC6_CHG_BALANCE");
        bc7ChgBalance = getValue("BC7_CHG_BALANCE");
        bc8ChgBalance = getValue("BC8_CHG_BALANCE");
        bc9ChgBalance = getValue("BC9_CHG_BALANCE");
        bc10ChgBalance = getValue("BC10_CHG_BALANCE");
        debits = getValue("DEBIT_AMOUNT");

        if (freeUnit != null && rateUse != null) {
            amounts = (Double.parseDouble(rateUse) - Double.parseDouble(freeUnit));
        }
        if (bc1Balance != null){
            if (bc1Balance.equalsIgnoreCase("2000") || bc1Balance.equalsIgnoreCase("3000") || bc1Balance.equalsIgnoreCase("7000")) {
                if (bc1ChgBalance != null) {
                    bc1c = bc1ChgBalance;
                } else {
                    bc1c = "0";
                }
            } else {
                bc1c = "0";
            }
        }
        if (bc2Balance != null){
            if (bc2Balance.equalsIgnoreCase("2000") || bc2Balance.equalsIgnoreCase("3000") || bc2Balance.equalsIgnoreCase("7000")) {
                if (bc2ChgBalance != null) {
                    bc2c = bc2ChgBalance;
                } else {
                    bc2c = "0";
                }
            } else {
                bc2c = "0";
            }
        }
        if (bc3Balance != null){
            if (bc3Balance.equalsIgnoreCase("2000") || bc3Balance.equalsIgnoreCase("3000") || bc3Balance.equalsIgnoreCase("7000")) {
                if (bc3ChgBalance != null) {
                    bc3c = bc3ChgBalance;
                } else {
                    bc3c = "0";
                }
            } else {
                bc3c = "0";
            }
        }
        if (bc4Balance != null){
            if (bc4Balance.equalsIgnoreCase("2000") || bc4Balance.equalsIgnoreCase("3000") || bc4Balance.equalsIgnoreCase("7000")) {
                if (bc4ChgBalance != null) {
                    bc4c = bc4ChgBalance;
                } else {
                    bc4c = "0";
                }
            } else {
                bc4c = "0";
            }
        }
        if (bc5Balance != null){
            if (bc5Balance.equalsIgnoreCase("2000") || bc5Balance.equalsIgnoreCase("3000") || bc5Balance.equalsIgnoreCase("7000")) {
                if (bc5ChgBalance != null) {
                    bc5c = bc5ChgBalance;
                } else {
                    bc5c = "0";
                }
            } else {
                bc5c = "0";
            }
        }
        if (bc6Balance != null){
            if (bc6Balance.equalsIgnoreCase("2000") || bc6Balance.equalsIgnoreCase("3000") || bc6Balance.equalsIgnoreCase("7000")) {
                if (bc6ChgBalance != null) {
                    bc6c = bc6ChgBalance;
                } else {
                    bc6c = "0";
                }
            } else {
                bc6c = "0";
            }
        }
        if (bc7Balance != null){
            if (bc7Balance.equalsIgnoreCase("2000") || bc7Balance.equalsIgnoreCase("3000") || bc7Balance.equalsIgnoreCase("7000")) {
                if (bc7ChgBalance != null) {
                    bc7c = bc7ChgBalance;
                } else {
                    bc7c = "0";
                }
            } else {
                bc7c = "0";
            }
        }
        if (bc8Balance != null){
            if (bc8Balance.equalsIgnoreCase("2000") || bc8Balance.equalsIgnoreCase("3000") || bc8Balance.equalsIgnoreCase("7000")) {
                if (bc8ChgBalance != null) {
                    bc8c = bc8ChgBalance;
                } else {
                    bc8c = "0";
                }
            } else {
                bc8c = "0";
            }
        }
        if (bc9Balance != null){
            if (bc9Balance.equalsIgnoreCase("2000") || bc9Balance.equalsIgnoreCase("3000") || bc9Balance.equalsIgnoreCase("7000")) {
                if (bc9ChgBalance != null) {
                    bc9c = bc9ChgBalance;
                } else {
                    bc9c = "0";
                }
            } else {
                bc9c = "0";
            }
        }
        if (bc10Balance != null){
            if (bc10Balance.equalsIgnoreCase("2000") || bc10Balance.equalsIgnoreCase("3000") || bc10Balance.equalsIgnoreCase("7000")) {
                if (bc10ChgBalance != null) {
                    bc10c = bc10ChgBalance;
                } else {
                    bc10c = "0";
                }
            } else {
                bc10c = "0";
            }
        }
        if (amounts != null && bc1c != null && bc2c != null && bc3c != null && bc4c != null && bc5c != null && bc6c != null && bc7c != null && bc8c != null && bc9c != null && bc10c != null){
            results = (amounts * (Integer.parseInt(bc1c)) + (Integer.parseInt(bc2c)) + (Integer.parseInt(bc3c)) + (Integer.parseInt(bc4c)) + (Integer.parseInt(bc5c)) + (Integer.parseInt(bc6c)) + (Integer.parseInt(bc7c)) + (Integer.parseInt(bc8c)) + (Integer.parseInt(bc9c)) + (Integer.parseInt(bc10c)));
        }

        if (debits != null && results != null){
            rateUsagePayg = results / Double.parseDouble(debits);
        }

        if (rateUsagePayg != null)
            return Optional.of(rateUsagePayg);
        return Optional.empty();
    }

    Double results2, results3, rateUsageBonus, amounts1;
    String bc1Chgbal, bc2Chgbal, bc3Chgbal, bc4Chgbal, bc5Chgbal, bc6Chgbal, bc7Chgbal, bc8Chgbal, bc9Chgbal, bc10Chgbal, rateUses, freeUnitAMountOfDur;
    String bc1BalType, bc2BalType, bc3BalType, bc4BalType, bc5BalType, bc6BalType, bc7BalType, bc8BalType, bc9BalType, bc10BalType, debitAmt;
    String bc1ChgBalan, bc2ChgBalan, bc3ChgBalan, bc4ChgBalan, bc5ChgBalan, bc6ChgBalan, bc7ChgBalan, bc8ChgBalan, bc9ChgBalan, bc10ChgBalan;

    public Optional<Double> getRateUsageBonus() {
        freeUnitAMountOfDur = getValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUses = getValue("RATE_USAGE");
        bc1BalType = getValue("BC1_BALANCE_TYPE");
        bc2BalType = getValue("BC2_BALANCE_TYPE");
        bc3BalType = getValue("BC3_BALANCE_TYPE");
        bc4BalType = getValue("BC4_BALANCE_TYPE");
        bc5BalType = getValue("BC5_BALANCE_TYPE");
        bc6BalType = getValue("BC6_BALANCE_TYPE");
        bc7BalType = getValue("BC7_BALANCE_TYPE");
        bc8BalType = getValue("BC8_BALANCE_TYPE");
        bc9BalType = getValue("BC9_BALANCE_TYPE");
        bc10BalType = getValue("BC10_BALANCE_TYPE");
        bc1ChgBalan = getValue("BC1_CHG_BALANCE");
        bc2ChgBalan = getValue("BC2_CHG_BALANCE");
        bc3ChgBalan = getValue("BC3_CHG_BALANCE");
        bc4ChgBalan = getValue("BC4_CHG_BALANCE");
        bc5ChgBalan = getValue("BC5_CHG_BALANCE");
        bc6ChgBalan = getValue("BC6_CHG_BALANCE");
        bc7ChgBalan = getValue("BC7_CHG_BALANCE");
        bc8ChgBalan = getValue("BC8_CHG_BALANCE");
        bc9ChgBalan = getValue("BC9_CHG_BALANCE");
        bc10ChgBalan = getValue("BC10_CHG_BALANCE");
        debitAmt = getValue("DEBIT_AMOUNT");

        if (freeUnitAMountOfDur != null && rateUse != null) {
            amounts1 = (Double.parseDouble(freeUnitAMountOfDuration) * Double.parseDouble(actualUsage)) / Double.parseDouble(rateUsage);
            results2 = Double.parseDouble(actualUsage) - amounts1;
        }
        if (bc1BalType != null){
            if ((!bc1BalType.equalsIgnoreCase("2000")) || (!bc1BalType.equalsIgnoreCase("3000")) || (!bc1BalType.equalsIgnoreCase("7000"))) {
                if (bc1ChgBalan != null) {
                    bc1Chgbal = bc1ChgBalan;
                } else {
                    bc1Chgbal = "0";
                }
            } else {
                bc1Chgbal = "0";
            }
        }
        if (bc2BalType != null){
            if ((!bc2BalType.equalsIgnoreCase("2000")) || (!bc2BalType.equalsIgnoreCase("3000")) || (!bc2BalType.equalsIgnoreCase("7000"))) {
                if (bc2ChgBalan != null) {
                    bc2Chgbal = bc2ChgBalan;
                } else {
                    bc2Chgbal = "0";
                }
            } else {
                bc2Chgbal = "0";
            }
        }
        if (bc3BalType != null){
            if ((!bc3BalType.equalsIgnoreCase("2000")) || (!bc3BalType.equalsIgnoreCase("3000")) || (!bc3BalType.equalsIgnoreCase("7000"))) {
                if (bc3ChgBalan != null) {
                    bc3Chgbal = bc3ChgBalan;
                } else {
                    bc3Chgbal = "0";
                }
            } else {
                bc3Chgbal = "0";
            }
        }
        if (bc4BalType != null){
            if ((!bc4BalType.equalsIgnoreCase("2000")) || (!bc4BalType.equalsIgnoreCase("3000")) || (!bc4BalType.equalsIgnoreCase("7000"))) {
                if (bc3ChgBalan != null) {
                    bc4Chgbal = bc3ChgBalan;
                } else {
                    bc4Chgbal = "0";
                }
            } else {
                bc4Chgbal = "0";
            }
        }
        if (bc5BalType != null){
            if ((!bc5BalType.equalsIgnoreCase("2000")) || (!bc5BalType.equalsIgnoreCase("3000")) || (!bc5BalType.equalsIgnoreCase("7000"))) {
                if (bc4ChgBalan != null) {
                    bc5Chgbal = bc4ChgBalan;
                } else {
                    bc5Chgbal = "0";
                }
            } else {
                bc5Chgbal = "0";
            }
        }
        if (bc6BalType != null){
            if ((!bc6BalType.equalsIgnoreCase("2000")) || (!bc6BalType.equalsIgnoreCase("3000")) || (!bc6BalType.equalsIgnoreCase("7000"))) {
                if (bc5ChgBalan != null) {
                    bc6Chgbal = bc5ChgBalan;
                } else {
                    bc6Chgbal = "0";
                }
            } else {
                bc6Chgbal = "0";
            }
        }
        if (bc7BalType != null){
            if ((!bc7BalType.equalsIgnoreCase("2000")) || (!bc7BalType.equalsIgnoreCase("3000")) || (!bc7BalType.equalsIgnoreCase("7000"))) {
                if (bc7ChgBalan != null) {
                    bc7Chgbal = bc7ChgBalan;
                } else {
                    bc7Chgbal = "0";
                }
            } else {
                bc7Chgbal = "0";
            }
        }
        if (bc6BalType != null){
            if ((!bc8BalType.equalsIgnoreCase("2000")) || (!bc8BalType.equalsIgnoreCase("3000")) || (!bc8BalType.equalsIgnoreCase("7000"))) {
                if (bc8ChgBalan != null) {
                    bc8Chgbal = bc8ChgBalan;
                } else {
                    bc8Chgbal = "0";
                }
            } else {
                bc8Chgbal = "0";
            }
        }
        if (bc9BalType != null){
            if ((!bc9BalType.equalsIgnoreCase("2000")) || (!bc9BalType.equalsIgnoreCase("3000")) || (!bc9BalType.equalsIgnoreCase("7000"))) {
                if (bc9ChgBalan != null) {
                    bc9Chgbal = bc9ChgBalan;
                } else {
                    bc9Chgbal = "0";
                }
            } else {
                bc9Chgbal = "0";
            }
        }
        if (bc10BalType != null){
            if ((!bc10BalType.equalsIgnoreCase("2000")) || (!bc10BalType.equalsIgnoreCase("3000")) || (!bc10BalType.equalsIgnoreCase("7000"))) {
                if (bc10ChgBalan != null) {
                    bc10Chgbal = bc10ChgBalan;
                } else {
                    bc10Chgbal = "0";
                }
            } else {
                bc10Chgbal = "0";
            }
        }
        if (results2 != null && bc1Chgbal != null && bc2Chgbal != null && bc3Chgbal != null && bc4Chgbal != null && bc5Chgbal != null && bc6Chgbal != null && bc7Chgbal != null && bc8Chgbal != null && bc9Chgbal != null && bc10Chgbal != null){
            results3 = (results2 * (Integer.parseInt(bc1Chgbal)) + (Integer.parseInt(bc2Chgbal)) + (Integer.parseInt(bc3Chgbal)) + (Integer.parseInt(bc4Chgbal)) + (Integer.parseInt(bc5Chgbal)) + (Integer.parseInt(bc6Chgbal)) + (Integer.parseInt(bc7Chgbal)) + (Integer.parseInt(bc8Chgbal)) + (Integer.parseInt(bc9Chgbal)) + (Integer.parseInt(bc10Chgbal)));
        }
        if (debitAmt != null && result3 != null){
            rateUsageBonus = results3 / Double.parseDouble(debitAmt);
        }

        if (rateUsageBonus != null)
            return Optional.of(rateUsageBonus);
        return Optional.empty();
    }

    String rateUsageAllowance;

    public Optional<String> getRateUsageAllowance() {
        rateUsageAllowance = getValue("FREE_UNIT_AMOUNT_OF_DURATION");

        if (rateUsageAllowance != null) {
            return Optional.of(rateUsageAllowance);
        }
        return Optional.empty();
    }

    String realRevenue, debitFromAdvancePrepaid, debitFromAdvancePostpaid, debitFromCreditPostpaid;

    public Optional<String> getRealRevenue() {
        debitFromAdvancePrepaid = getValue("Debit_From_Advance_Prepaid");
        debitFromAdvancePostpaid = getValue("Debit_From_Advance_Postpaid");
        debitFromCreditPostpaid = getValue("Debit_From_Credit_Postpaid");

        if (debitFromAdvancePrepaid != null && debitFromAdvancePostpaid != null && debitFromCreditPostpaid != null) {
            realRevenue = debitFromAdvancePrepaid + debitFromCreditPostpaid + debitFromCreditPostpaid;
        }
        if (realRevenue != null)
            return Optional.of(realRevenue);
        return Optional.empty();
    }

    String dialed, dialedNumber;

    public Optional<String> getDialedNumber() {
        dialed = getValue("DialedNumber");
        if (dialed != null) {
            dialedNumber = "966" + dialed;
        }
        if (dialedNumber != null)
            return Optional.of(dialedNumber);
        return Optional.empty();
    }

    String eventDirectionKey, serviceFlow;

    public Optional<String> getEventDirectionKey() {
        serviceFlow = getValue("ServiceFlow");
        if (serviceFlow != null) {
            switch (serviceFlow) {
                case "1":
                case "3":
                    eventDirectionKey = "1";
                    break;
                case "2":
                    eventDirectionKey = "2";
                    break;
            }
        }
        if (eventDirectionKey != null)
            return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    String servedType, roamState, payType;

    public Optional<String> getServedType() {
        payType = getValue("PayType");
        roamState = getValue("RoamState");

        if (roamState != null) {
            switch (roamState) {
                case "3":
                    switch (payType) {
                        case "1":
                            servedType = "5";
                            break;
                        case "0":
                            servedType = "6";
                            break;
                        case "3":
                            servedType = "8";
                            break;
                    }
                    break;
                case "0":
                    switch (payType) {
                        case "0":
                            servedType = "2";
                            break;
                        case "1":
                            servedType = "1";
                            break;
                        case "3":
                            servedType = "7";
                            break;
                    }
                    break;
                default:
                    servedType = payType;
                    break;
            }
        }
        if (servedType != null)
            return Optional.of(servedType);
        return Optional.empty();
    }

    Date chargingTime;

    public Optional<String> getChargingTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                chargingTime = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(chargingTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    String onlineChargingFlag;

    public Optional<String> getOnlineChargingFlag() {
        onlineChargingFlag = getValue("OnlineChargingFlag");
        if (onlineChargingFlag != null) {
            switch (onlineChargingFlag) {
                case "1":
                    onlineChargingFlag = "Online";
                    break;
                case "2":
                    onlineChargingFlag = "offline";
                    break;
                default:
                    onlineChargingFlag = "unknown";
                    break;
            }
        }

        if (onlineChargingFlag != null)
            return Optional.of(onlineChargingFlag);
        return Optional.empty();
    }

    Date startTimeOfBill;

    public Optional<String> getStartTimeOfBillCycle(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                startTimeOfBill = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(startTimeOfBill));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    String groupPayFlag;

    public Optional<String> getGroupPayFlag() {
        groupPayFlag = getValue("GroupPayFlag");
        if (groupPayFlag != null) {
            switch (groupPayFlag) {
                case "0":
                    groupPayFlag = "Personal";
                    break;
                case "1":
                    groupPayFlag = "Group";
                    break;
                default:
                    groupPayFlag = "corp/personal";
                    break;
            }
        }

        if (groupPayFlag != null)
            return Optional.of(groupPayFlag);
        return Optional.empty();
    }

    String charge;

    public Optional<String> getCharge(String DEBIT_AMOUNT) {
        charge = getValue(DEBIT_AMOUNT);
        if (charge != null) {
            try {
                Double.parseDouble(charge);
                return Optional.of(charge);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    String zeroDurationInd;

    public Optional<String> getZeroDurationInd() {
        zeroDurationInd = getValue("ACTUAL_USAGE");

        if (zeroDurationInd != null) {
            return Optional.of(zeroDurationInd);
        }
        return Optional.empty();
    }

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }

    String normalizeMSISDN(String number) {
        if (number != null) {
            if (number.startsWith("0")) {
                number = ltrim(number, '0');
            }
            if (number.length() < 10) {
                if (number.startsWith("966")) {
                    return number;
                } else {
                    number = "966" + number;
                }
            }
            return number;
        }
        return "";
    }
}
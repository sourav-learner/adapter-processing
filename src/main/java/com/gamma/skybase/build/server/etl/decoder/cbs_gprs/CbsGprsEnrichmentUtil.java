package com.gamma.skybase.build.server.etl.decoder.cbs_gprs;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class CbsGprsEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    LinkedHashMap<String, Object> rec;

    private CbsGprsEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsGprsEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsGprsEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    public double getDoubleValue(String field) {
        Object value = rec.get(field);

        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
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
                    status = "-99";
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
        }
        catch (Exception e){
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
                    objType = "-99";
                    break;
            }
        }
        if (objType != null)
            return Optional.of(objType);
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

    String eventDirectionKey;

    public Optional<String> getEventDirectionKey() {
        eventDirectionKey = getValue("ServiceFlow");
        if (eventDirectionKey != null) {
            switch (eventDirectionKey) {
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

    String upFlux;
    Long uploadVolume;


    public Optional<Long> getUploadVolume(String UpFlux) {
        upFlux = getValue(UpFlux);
        if (upFlux != null && !upFlux.isEmpty()) {
            try {
                uploadVolume = Long.parseLong(upFlux) / 1024;
                return Optional.of(uploadVolume);
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String downFlux;
    Long downloadVolume;

    public Optional<Long> getDownloadVolume(String DownFlux) {
        downFlux = getValue(DownFlux);
        if (downFlux != null) {
            try {
                downloadVolume = Long.valueOf(downFlux) / 1024;
                return Optional.of(downloadVolume);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    Long totalVolume;
    String totalFlux;

    public Optional<Long> getTotalVol(String TotalFlux) {
        totalFlux = getValue(TotalFlux);
        if (totalFlux != null) {
            try {
                totalVolume = Long.valueOf(totalFlux) / 1024;
                return Optional.of(totalVolume);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    String dialed, chargingPartyNumber;

    public Optional<String> getChargingPartyNumber() {
        dialed = getValue("ChargingPartyNumber");
        if (dialed != null) {
            chargingPartyNumber = normalizeMSISDN(dialed);
        }
        if (chargingPartyNumber != null)
            return Optional.of(chargingPartyNumber);
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
                        case "2":
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
                        case "2":
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
                    onlineChargingFlag = "-99";
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

    Double charge;

    public Optional<Double> getCharge(String DEBIT_AMOUNT) {
        String temp = getValue(DEBIT_AMOUNT);
        if (temp != null) {
            try {
                charge = Double.parseDouble(temp) / 1000;
                return Optional.of(charge);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    Double actualUsage, freeUnitAMountOfDuration, rateUsage, bc1BalanceType, bc2BalanceType, bc3BalanceType, bc4BalanceType, bc5BalanceType, bc6BalanceType, bc7BalanceType, bc8BalanceType, bc9BalanceType, bc10BalanceType;
    Double bc1ChgeBalance, bc2ChgeBalance, bc3ChgeBalance, bc4ChgeBalance, bc5ChgeBalance, bc6ChgeBalance, bc7ChgeBalance, bc8ChgeBalance, bc9ChgeBalance, bc10ChgeBalance, debitAmount;

    Double actualUsagePayg;

    public Optional<Double> getActualUsagePayg() {
        actualUsage = getDoubleValue("ACTUAL_USAGE");
        freeUnitAMountOfDuration = getDoubleValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUsage = getDoubleValue("RATE_USAGE");
        bc1BalanceType = getDoubleValue("BC1_BALANCE_TYPE");
        bc2BalanceType = getDoubleValue("BC2_BALANCE_TYPE");
        bc3BalanceType = getDoubleValue("BC3_BALANCE_TYPE");
        bc4BalanceType = getDoubleValue("BC4_BALANCE_TYPE");
        bc5BalanceType = getDoubleValue("BC5_BALANCE_TYPE");
        bc6BalanceType = getDoubleValue("BC6_BALANCE_TYPE");
        bc7BalanceType = getDoubleValue("BC7_BALANCE_TYPE");
        bc8BalanceType = getDoubleValue("BC8_BALANCE_TYPE");
        bc9BalanceType = getDoubleValue("BC9_BALANCE_TYPE");
        bc10BalanceType = getDoubleValue("BC10_BALANCE_TYPE");
        bc1ChgeBalance = getDoubleValue("BC1_CHG_BALANCE");
        bc2ChgeBalance = getDoubleValue("BC2_CHG_BALANCE");
        bc3ChgeBalance = getDoubleValue("BC3_CHG_BALANCE");
        bc4ChgeBalance = getDoubleValue("BC4_CHG_BALANCE");
        bc5ChgeBalance = getDoubleValue("BC5_CHG_BALANCE");
        bc6ChgeBalance = getDoubleValue("BC6_CHG_BALANCE");
        bc7ChgeBalance = getDoubleValue("BC7_CHG_BALANCE");
        bc8ChgeBalance = getDoubleValue("BC8_CHG_BALANCE");
        bc9ChgeBalance = getDoubleValue("BC9_CHG_BALANCE");
        bc10ChgeBalance = getDoubleValue("BC10_CHG_BALANCE");
        debitAmount = getDoubleValue("DEBIT_AMOUNT");

        actualUsagePayg = (actualUsage - (freeUnitAMountOfDuration * actualUsage / rateUsage))
                * (
                ((bc1BalanceType == 2000 || bc1BalanceType == 3000 || bc1BalanceType == 7000) ? (bc1ChgeBalance.toString() == null ? 0 : bc1ChgeBalance) : 0)
                        + ((bc2BalanceType == 2000 || bc2BalanceType == 3000 || bc2BalanceType == 7000) ? (bc2ChgeBalance.toString() == null ? 0 : bc2ChgeBalance) : 0)
                        + ((bc3BalanceType == 2000 || bc3BalanceType == 3000 || bc3BalanceType == 7000) ? (bc3ChgeBalance.toString() == null ? 0 : bc3ChgeBalance) : 0)
                        + ((bc4BalanceType == 2000 || bc4BalanceType == 3000 || bc4BalanceType == 7000) ? (bc4ChgeBalance.toString() == null ? 0 : bc4ChgeBalance) : 0)
                        + ((bc5BalanceType == 2000 || bc5BalanceType == 3000 || bc5BalanceType == 7000) ? (bc5ChgeBalance.toString() == null ? 0 : bc5ChgeBalance) : 0)
                        + ((bc6BalanceType == 2000 || bc6BalanceType == 3000 || bc6BalanceType == 7000) ? (bc6ChgeBalance.toString() == null ? 0 : bc6ChgeBalance) : 0)
                        + ((bc7BalanceType == 2000 || bc7BalanceType == 3000 || bc7BalanceType == 7000) ? (bc7ChgeBalance.toString() == null ? 0 : bc8ChgeBalance) : 0)
                        + ((bc8BalanceType == 2000 || bc8BalanceType == 3000 || bc8BalanceType == 7000) ? (bc8ChgeBalance.toString() == null ? 0 : bc9ChgeBalance) : 0)
                        + ((bc9BalanceType == 2000 || bc9BalanceType == 3000 || bc9BalanceType == 7000) ? (bc9ChgeBalance.toString() == null ? 0 : bc9ChgeBalance) : 0)
                        + ((bc10BalanceType == 2000 || bc10BalanceType == 3000 || bc10BalanceType == 7000) ? (bc10ChgeBalance.toString() == null ? 0 : bc10ChgeBalance) : 0))
                / debitAmount;

        if (Double.isNaN(actualUsagePayg)) {
            return Optional.of(0.0);
        } else {
            return Optional.of(actualUsagePayg);
        }
    }

    Double actualUsageBonus, actualUsg, freeUnitAmount, rateUsg, bc1ChgBal, bc2ChgBal, bc3ChgBal, bc4ChgBal, bc5ChgBal, bc6ChgBal, bc7ChgBal, bc8ChgBal, bc9ChgBal, bc10ChgBal;
    Double bc1Bal, bc2Bal, bc3Bal, bc4Bal, bc5Bal, bc6Bal, bc7Bal, bc8Bal, bc9Bal, bc10Bal, debitAmounts;
    Double bc1Chg, bc2Chg, bc3Chg, bc4Chg, bc5Chg, bc6Chg, bc7Chg, bc8Chg, bc9Chg, bc10Chg;

    public Optional<Double> getActualUsageBonus() {
        actualUsg = getDoubleValue("ACTUAL_USAGE");
        freeUnitAmount = getDoubleValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUsg = getDoubleValue("RATE_USAGE");
        bc1Bal = getDoubleValue("BC1_BALANCE_TYPE");
        bc2Bal = getDoubleValue("BC2_BALANCE_TYPE");
        bc3Bal = getDoubleValue("BC3_BALANCE_TYPE");
        bc4Bal = getDoubleValue("BC4_BALANCE_TYPE");
        bc5Bal = getDoubleValue("BC5_BALANCE_TYPE");
        bc6Bal = getDoubleValue("BC6_BALANCE_TYPE");
        bc7Bal = getDoubleValue("BC7_BALANCE_TYPE");
        bc8Bal = getDoubleValue("BC8_BALANCE_TYPE");
        bc9Bal = getDoubleValue("BC9_BALANCE_TYPE");
        bc10Bal = getDoubleValue("BC10_BALANCE_TYPE");
        bc1ChgBal = getDoubleValue("BC1_CHG_BALANCE");
        bc2ChgBal = getDoubleValue("BC2_CHG_BALANCE");
        bc3ChgBal = getDoubleValue("BC3_CHG_BALANCE");
        bc4ChgBal = getDoubleValue("BC4_CHG_BALANCE");
        bc5ChgBal = getDoubleValue("BC5_CHG_BALANCE");
        bc6ChgBal = getDoubleValue("BC6_CHG_BALANCE");
        bc7ChgBal = getDoubleValue("BC7_CHG_BALANCE");
        bc8ChgBal = getDoubleValue("BC8_CHG_BALANCE");
        bc9ChgBal = getDoubleValue("BC9_CHG_BALANCE");
        bc10ChgBal = getDoubleValue("BC10_CHG_BALANCE");
        debitAmounts = getDoubleValue("DEBIT_AMOUNT");

        Double actualUsageBonus1 = (actualUsg - (freeUnitAmount * actualUsg / rateUsg));
        if (bc1Bal != 2000) {
            if (bc1Bal != 3000) {
                if (bc1Bal != 7000) {
                    if (bc1ChgBal != null) {
                        bc1Chg = bc1ChgBal;
                    } else {
                        bc1Chg = 0.0;
                    }
                } else {
                    bc1Chg = 0.0;
                }
            } else {
                bc1Chg = 0.0;
            }
        } else {
            bc1Chg = 0.0;
        }

        if (bc2Bal != 2000) {
            if (bc2Bal != 3000) {
                if (bc2Bal != 7000) {
                    if (bc2ChgBal != null) {
                        bc2Chg = bc2ChgBal;
                    } else {
                        bc2Chg = 0.0;
                    }
                } else {
                    bc2Chg = 0.0;
                }
            } else {
                bc2Chg = 0.0;
            }
        } else {
            bc2Chg = 0.0;
        }

        if (bc3Bal != 2000) {
            if (bc3Bal != 3000) {
                if (bc3Bal != 7000) {
                    if (bc3ChgBal != null) {
                        bc3Chg = bc3ChgBal;
                    } else {
                        bc3Chg = 0.0;
                    }
                } else {
                    bc3Chg = 0.0;
                }
            } else {
                bc3Chg = 0.0;
            }
        } else {
            bc3Chg = 0.0;
        }

        if (bc4Bal != 2000) {
            if (bc4Bal != 3000) {
                if (bc4Bal != 7000) {
                    if (bc4ChgBal != null) {
                        bc4Chg = bc4ChgBal;
                    } else {
                        bc4Chg = 0.0;
                    }
                } else {
                    bc4Chg = 0.0;
                }
            } else {
                bc4Chg = 0.0;
            }
        } else {
            bc4Chg = 0.0;
        }

        if (bc5Bal != 2000) {
            if (bc5Bal != 3000) {
                if (bc5Bal != 7000) {
                    if (bc5ChgBal != null) {
                        bc5Chg = bc5ChgBal;
                    } else {
                        bc5Chg = 0.0;
                    }
                } else {
                    bc5Chg = 0.0;
                }
            } else {
                bc5Chg = 0.0;
            }
        } else {
            bc5Chg = 0.0;
        }

        if (bc6Bal != 2000) {
            if (bc6Bal != 3000) {
                if (bc6Bal != 7000) {
                    if (bc6ChgBal != null) {
                        bc6Chg = bc6ChgBal;
                    } else {
                        bc6Chg = 0.0;
                    }
                } else {
                    bc6Chg = 0.0;
                }
            } else {
                bc6Chg = 0.0;
            }
        } else {
            bc6Chg = 0.0;
        }

        if (bc7Bal != 2000) {
            if (bc7Bal != 3000) {
                if (bc7Bal != 7000) {
                    if (bc7ChgBal != null) {
                        bc7Chg = bc7ChgBal;
                    } else {
                        bc7Chg = 0.0;
                    }
                } else {
                    bc7Chg = 0.0;
                }
            } else {
                bc7Chg = 0.0;
            }
        } else {
            bc7Chg = 0.0;
        }

        if (bc8Bal != 2000) {
            if (bc8Bal != 3000) {
                if (bc8Bal != 7000) {
                    if (bc8ChgBal != null) {
                        bc8Chg = bc8ChgBal;
                    } else {
                        bc8Chg = 0.0;
                    }
                } else {
                    bc8Chg = 0.0;
                }
            } else {
                bc8Chg = 0.0;
            }
        } else {
            bc8Chg = 0.0;
        }

        if (bc9Bal != 2000) {
            if (bc9Bal != 3000) {
                if (bc9Bal != 7000) {
                    if (bc9ChgBal != null) {
                        bc9Chg = bc9ChgBal;
                    } else {
                        bc9Chg = 0.0;
                    }
                } else {
                    bc9Chg = 0.0;
                }
            } else {
                bc9Chg = 0.0;
            }
        } else {
            bc9Chg = 0.0;
        }

        if (bc10Bal != 2000) {
            if (bc10Bal != 3000) {
                if (bc10Bal != 7000) {
                    if (bc10ChgBal != null) {
                        bc10Chg = bc10ChgBal;
                    } else {
                        bc10Chg = 0.0;
                    }
                } else {
                    bc10Chg = 0.0;
                }
            } else {
                bc10Chg = 0.0;
            }
        } else {
            bc10Chg = 0.0;
        }

        actualUsageBonus = (actualUsageBonus1 * (bc1Chg + bc2Chg + bc3Chg + bc4Chg + bc5Chg + bc6Chg + bc7Chg + bc8Chg + bc9Chg + bc10Chg)) / debitAmounts;

        if (Double.isNaN(actualUsageBonus)) {
            return Optional.of(0.0);
        } else {
            return Optional.of(actualUsageBonus);
        }
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
        if (Double.isNaN(allowance)) {
            return Optional.of(0.0);
        } else {
            return Optional.of(allowance);
        }
    }

    Double rateUse, freeUnit, bc1Balance, bc2Balance, bc3Balance, bc4Balance, bc5Balance, bc6Balance, bc7Balance, bc8Balance, bc9Balance, bc10Balance;
    Double bc1ChgBalance, bc2ChgBalance, bc3ChgBalance, bc4ChgBalance, bc5ChgBalance, bc6ChgBalance, bc7ChgBalance, bc8ChgBalance, bc9ChgBalance, bc10ChgBalance, debits, rateUsagePayg;

    public Optional<Double> getRateUsagePayg() {
        rateUse = getDoubleValue("RATE_USAGE");
        freeUnit = getDoubleValue("FREE_UNIT_AMOUNT_OF_DURATION");
        bc1Balance = getDoubleValue("BC1_BALANCE_TYPE");
        bc2Balance = getDoubleValue("BC2_BALANCE_TYPE");
        bc3Balance = getDoubleValue("BC3_BALANCE_TYPE");
        bc4Balance = getDoubleValue("BC4_BALANCE_TYPE");
        bc5Balance = getDoubleValue("BC5_BALANCE_TYPE");
        bc6Balance = getDoubleValue("BC6_BALANCE_TYPE");
        bc7Balance = getDoubleValue("BC7_BALANCE_TYPE");
        bc8Balance = getDoubleValue("BC8_BALANCE_TYPE");
        bc9Balance = getDoubleValue("BC9_BALANCE_TYPE");
        bc10Balance = getDoubleValue("BC10_BALANCE_TYPE");
        bc1ChgBalance = getDoubleValue("BC1_CHG_BALANCE");
        bc2ChgBalance = getDoubleValue("BC2_CHG_BALANCE");
        bc3ChgBalance = getDoubleValue("BC3_CHG_BALANCE");
        bc4ChgBalance = getDoubleValue("BC4_CHG_BALANCE");
        bc5ChgBalance = getDoubleValue("BC5_CHG_BALANCE");
        bc6ChgBalance = getDoubleValue("BC6_CHG_BALANCE");
        bc7ChgBalance = getDoubleValue("BC7_CHG_BALANCE");
        bc8ChgBalance = getDoubleValue("BC8_CHG_BALANCE");
        bc9ChgBalance = getDoubleValue("BC9_CHG_BALANCE");
        bc10ChgBalance = getDoubleValue("BC10_CHG_BALANCE");
        debits = getDoubleValue("DEBIT_AMOUNT");

        rateUsagePayg = (rateUse - freeUnit)
                * (
                ((bc1Balance == 2000 || bc1Balance == 3000 || bc1Balance == 7000) ? (bc1ChgBalance.toString() == null ? 0 : bc1ChgBalance) : 0)
                        + ((bc2Balance == 2000 || bc2Balance == 3000 || bc2Balance == 7000) ? (bc2ChgBalance.toString() == null ? 0 : bc2ChgBalance) : 0)
                        + ((bc3Balance == 2000 || bc3Balance == 3000 || bc3Balance == 7000) ? (bc3ChgBalance.toString() == null ? 0 : bc3ChgBalance) : 0)
                        + ((bc4Balance == 2000 || bc4Balance == 3000 || bc4Balance == 7000) ? (bc4ChgBalance.toString() == null ? 0 : bc4ChgBalance) : 0)
                        + ((bc5Balance == 2000 || bc5Balance == 3000 || bc5Balance == 7000) ? (bc5ChgBalance.toString() == null ? 0 : bc5ChgBalance) : 0)
                        + ((bc6Balance == 2000 || bc6Balance == 3000 || bc6Balance == 7000) ? (bc6ChgBalance.toString() == null ? 0 : bc6ChgBalance) : 0)
                        + ((bc7Balance == 2000 || bc7Balance == 3000 || bc7Balance == 7000) ? (bc7ChgBalance.toString() == null ? 0 : bc7ChgBalance) : 0)
                        + ((bc8Balance == 2000 || bc8Balance == 3000 || bc8Balance == 7000) ? (bc8ChgBalance.toString() == null ? 0 : bc8ChgBalance) : 0)
                        + ((bc9Balance == 2000 || bc9Balance == 3000 || bc9Balance == 7000) ? (bc9ChgBalance.toString() == null ? 0 : bc9ChgBalance) : 0)
                        + ((bc10Balance == 2000 || bc10Balance == 3000 || bc10Balance == 7000) ? (bc10ChgBalance.toString() == null ? 0 : bc10ChgBalance) : 0))
                / debits;

        if (Double.isNaN(rateUsagePayg)) {
            return Optional.of(0.0);
        } else {
            return Optional.of(rateUsagePayg);
        }
    }

    Double rateUses, freeUnitAMountOfDur, rateUsageBonus, rateUsageBonus1;
    Double bc1BalType, bc2BalType, bc3BalType, bc4BalType, bc5BalType, bc6BalType, bc7BalType, bc8BalType, bc9BalType, bc10BalType, debitAmt;
    Double bc1ChgBalan, bc2ChgBalan, bc3ChgBalan, bc4ChgBalan, bc5ChgBalan, bc6ChgBalan, bc7ChgBalan, bc8ChgBalan, bc9ChgBalan, bc10ChgBalan;
    Double bc1cBal, bc2cBal, bc3cBal, bc4cBal, bc5cBal, bc6cBal, bc7cBal, bc8cBal, bc9cBal, bc10cBal;

    public Optional<Double> getRateUsageBonus() {
        freeUnitAMountOfDur = getDoubleValue("FREE_UNIT_AMOUNT_OF_DURATION");
        rateUses = getDoubleValue("RATE_USAGE");
        bc1BalType = getDoubleValue("BC1_BALANCE_TYPE");
        bc2BalType = getDoubleValue("BC2_BALANCE_TYPE");
        bc3BalType = getDoubleValue("BC3_BALANCE_TYPE");
        bc4BalType = getDoubleValue("BC4_BALANCE_TYPE");
        bc5BalType = getDoubleValue("BC5_BALANCE_TYPE");
        bc6BalType = getDoubleValue("BC6_BALANCE_TYPE");
        bc7BalType = getDoubleValue("BC7_BALANCE_TYPE");
        bc8BalType = getDoubleValue("BC8_BALANCE_TYPE");
        bc9BalType = getDoubleValue("BC9_BALANCE_TYPE");
        bc10BalType = getDoubleValue("BC10_BALANCE_TYPE");
        bc1ChgBalan = getDoubleValue("BC1_CHG_BALANCE");
        bc2ChgBalan = getDoubleValue("BC2_CHG_BALANCE");
        bc3ChgBalan = getDoubleValue("BC3_CHG_BALANCE");
        bc4ChgBalan = getDoubleValue("BC4_CHG_BALANCE");
        bc5ChgBalan = getDoubleValue("BC5_CHG_BALANCE");
        bc6ChgBalan = getDoubleValue("BC6_CHG_BALANCE");
        bc7ChgBalan = getDoubleValue("BC7_CHG_BALANCE");
        bc8ChgBalan = getDoubleValue("BC8_CHG_BALANCE");
        bc9ChgBalan = getDoubleValue("BC9_CHG_BALANCE");
        bc10ChgBalan = getDoubleValue("BC10_CHG_BALANCE");
        debitAmt = getDoubleValue("DEBIT_AMOUNT");

        rateUsageBonus1 = (rateUses - freeUnitAMountOfDur);

        if (bc1BalType != 2000) {
            if (bc1BalType != 3000) {
                if (bc1BalType != 7000) {
                    if (bc1ChgBalan != null) {
                        bc1cBal = bc1ChgBalan;
                    } else {
                        bc1cBal = 0.0;
                    }
                } else {
                    bc1cBal = 0.0;
                }
            } else {
                bc1cBal = 0.0;
            }
        } else {
            bc1cBal = 0.0;
        }

        if (bc2BalType != 2000) {
            if (bc2BalType != 3000) {
                if (bc2BalType != 7000) {
                    if (bc2ChgBalan != null) {
                        bc2cBal = bc2ChgBalan;
                    } else {
                        bc2cBal = 0.0;
                    }
                } else {
                    bc2cBal = 0.0;
                }
            } else {
                bc2cBal = 0.0;
            }
        } else {
            bc2cBal = 0.0;
        }

        if (bc3BalType != 2000) {
            if (bc3BalType != 3000) {
                if (bc3BalType != 7000) {
                    if (bc3ChgBalan != null) {
                        bc3cBal = bc3ChgBalan;
                    } else {
                        bc3cBal = 0.0;
                    }
                } else {
                    bc3cBal = 0.0;
                }
            } else {
                bc3cBal = 0.0;
            }
        } else {
            bc3cBal = 0.0;
        }

        if (bc4BalType != 2000) {
            if (bc4BalType != 3000) {
                if (bc4BalType != 7000) {
                    if (bc4ChgBalan != null) {
                        bc4cBal = bc4ChgBalan;
                    } else {
                        bc4cBal = 0.0;
                    }
                } else {
                    bc4cBal = 0.0;
                }
            } else {
                bc4cBal = 0.0;
            }
        } else {
            bc4cBal = 0.0;
        }

        if (bc5BalType != 2000) {
            if (bc5BalType != 3000) {
                if (bc5BalType != 7000) {
                    if (bc5ChgBalan != null) {
                        bc5cBal = bc5ChgBalan;
                    } else {
                        bc5cBal = 0.0;
                    }
                } else {
                    bc5cBal = 0.0;
                }
            } else {
                bc5cBal = 0.0;
            }
        } else {
            bc5cBal = 0.0;
        }

        if (bc6BalType != 2000) {
            if (bc6BalType != 3000) {
                if (bc6BalType != 7000) {
                    if (bc6ChgBalan != null) {
                        bc6cBal = bc6ChgBalan;
                    } else {
                        bc6cBal = 0.0;
                    }
                } else {
                    bc6cBal = 0.0;
                }
            } else {
                bc6cBal = 0.0;
            }
        } else {
            bc6cBal = 0.0;
        }

        if (bc7BalType != 2000) {
            if (bc7BalType != 3000) {
                if (bc7BalType != 7000) {
                    if (bc7ChgBalan != null) {
                        bc7cBal = bc7ChgBalan;
                    } else {
                        bc7cBal = 0.0;
                    }
                } else {
                    bc7cBal = 0.0;
                }
            } else {
                bc7cBal = 0.0;
            }
        } else {
            bc7cBal = 0.0;
        }

        if (bc8BalType != 2000) {
            if (bc8BalType != 3000) {
                if (bc8BalType != 7000) {
                    if (bc8ChgBalan != null) {
                        bc8cBal = bc8ChgBalan;
                    } else {
                        bc8cBal = 0.0;
                    }
                } else {
                    bc8cBal = 0.0;
                }
            } else {
                bc8cBal = 0.0;
            }
        } else {
            bc8cBal = 0.0;
        }

        if (bc9BalType != 2000) {
            if (bc9BalType != 3000) {
                if (bc9BalType != 7000) {
                    if (bc9ChgBalan != null) {
                        bc9cBal = bc9ChgBalan;
                    } else {
                        bc9cBal = 0.0;
                    }
                } else {
                    bc9cBal = 0.0;
                }
            } else {
                bc9cBal = 0.0;
            }
        } else {
            bc9cBal = 0.0;
        }

        if (bc10BalType != 2000) {
            if (bc10BalType != 3000) {
                if (bc10BalType != 7000) {
                    if (bc10ChgBalan != null) {
                        bc10cBal = bc10ChgBalan;
                    } else {
                        bc10cBal = 0.0;
                    }
                } else {
                    bc10cBal = 0.0;
                }
            } else {
                bc10cBal = 0.0;
            }
        } else {
            bc10cBal = 0.0;
        }

        rateUsageBonus = (rateUsageBonus1 * (bc1cBal + bc2cBal + bc3cBal + bc4cBal + bc5cBal + bc6cBal + bc7cBal + bc8cBal + bc9cBal + bc10cBal)) / debitAmt;

        if (Double.isNaN(rateUsageBonus)) {
            return Optional.of(0.0);
        } else {
            return Optional.of(rateUsageBonus);
        }
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

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }

}
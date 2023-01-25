package com.gamma.skybase.build.server.etl.tx.cbs_voice;

import com.gamma.components.commons.app.AppConfig;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class cbsVoiceEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private cbsVoiceEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static cbsVoiceEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new cbsVoiceEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
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
                    onlineChargingFlag = "unknown";
                    break;
            }
        }

        if (onlineChargingFlag != null)
            return Optional.of(onlineChargingFlag);
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

    String eventDirectionKey , serviceFlow;

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

    String zeroDurationInd;

    public Optional<String> getZeroDurationInd() {
        zeroDurationInd = getValue("ACTUAL_USAGE");

        if (zeroDurationInd != null) {
            return Optional.of(zeroDurationInd);
        }
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
}
package com.gamma.skybase.build.server.etl.tx.cbs_voice;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import com.gamma.telco.utility.reference.ReferenceDimTadigLookup;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CBS_VOICEEnrichmentUtil {
    private final AppConfig appConfig = AppConfig.instance();
    private final String homePlmnCode = appConfig.getProperty("app.datasource.plmn");
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private CBS_VOICEEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CBS_VOICEEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CBS_VOICEEnrichmentUtil(record);
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

    String onlineChargingFlag;
//    if 1 then 'Online' elsif 2 then ''offline' else 'unknown'

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

    String zeroChargeInd;

    public Optional<String> getZeroChargeInd() {
        zeroChargeInd = getValue("ACTUAL_USAGE");
        if (zeroChargeInd != null) {
            switch (zeroChargeInd) {
                case "0":
                    zeroChargeInd = "1";
                    break;
                default:
                    zeroChargeInd = "0";
            }
        }

        if (zeroChargeInd != null)
            return Optional.of(zeroChargeInd);
        return Optional.empty();
    }

    String zeroDurationInd;

    public Optional<String> getZeroDurationInd() {
        zeroDurationInd = getValue("ACTUAL_USAGE");
        if (zeroDurationInd != null) {
            switch (zeroDurationInd) {
                case "0":
                    zeroDurationInd = "1";
                    break;
                default:
                    zeroDurationInd = "0";
            }
        }

        if (zeroDurationInd != null)
            return Optional.of(zeroDurationInd);
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

    public Optional<String> getOtherMSISDN() {
        return Optional.empty();
    }

    public Optional<String> getServedMSISDN() {
        return Optional.empty();
    }

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }
}
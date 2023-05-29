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
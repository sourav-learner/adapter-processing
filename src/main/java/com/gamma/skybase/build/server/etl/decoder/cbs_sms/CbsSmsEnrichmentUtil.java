package com.gamma.skybase.build.server.etl.decoder.cbs_sms;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.gamma.telco.utility.TelcoEnrichmentUtility.ltrim;

public class CbsSmsEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));


    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));

    private final ThreadLocal<DateTimeFormatter> dtfT = ThreadLocal.withInitial(() ->
            DateTimeFormatter.ofPattern("ddMMyyyy HH:mm:ss"));
    private final ThreadLocal<DateTimeFormatter> dtfS = ThreadLocal.withInitial(() ->
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

    LinkedHashMap<String, Object> rec;
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();

    private CbsSmsEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsSmsEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsSmsEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    LocalDateTime callStartTime;
    String genFullDate;

    public Optional<String> getStartTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
//                callStartTime = sdfS.get().parse(s);
//                genFullDate = fullDate.get().format(callStartTime);
//                return Optional.of(sdfT.get().format(callStartTime));

                callStartTime = LocalDateTime.parse(s, dtfS.get());
                return Optional.of(callStartTime.format(dtfT.get()));
            }
        } catch (Exception e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    LocalDateTime callEndTime;

    public Optional<String> getEndTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
//                callEndTime = sdfS.get().parse(s);
//                return Optional.of(sdfT.get().format(callEndTime));
                callEndTime = LocalDateTime.parse(s, dtfS.get());
                return Optional.of(callEndTime.format(dtfT.get()));
            }
        } catch (Exception e) {
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

    String sendResult;

    public Optional<String> getSendResult() {
        sendResult = getValue("SendResult");
        if (sendResult != null) {
            switch (sendResult) {
                case "0":
                    sendResult = "Success";
                    break;
                default:
                    sendResult = "VALUE";
                    break;
            }
        }
        if (sendResult != null)
            return Optional.of(sendResult);
        return Optional.empty();
    }

    String refundIndicator;

    public Optional<String> getRefundIndicator() {
        refundIndicator = getValue("RefundIndicator");
        if (refundIndicator != null) {
            switch (refundIndicator) {
                case "0":
                    refundIndicator = "Fee Deduction";
                    break;
                case "1":
                    refundIndicator = "Refund";
                    break;
                default:
                    refundIndicator = "-99";
                    break;
            }
        }
        if (refundIndicator != null)
            return Optional.of(refundIndicator);
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

    String OnNetIndicator;

    Optional<String> getOnNetIndicator() {
        OnNetIndicator = getValue("OnNetIndicator");
        if (OnNetIndicator != null) {
            switch (OnNetIndicator) {
                case "0":
                    OnNetIndicator = "INTRA N/W";
                    break;
                case "1":
                    OnNetIndicator = "INTER N/W";
                    break;
                default:
                    OnNetIndicator = "-99";
                    break;
            }
        }
        if (OnNetIndicator != null)
            return Optional.of(OnNetIndicator);
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
//                startTimeOfBill = sdfS.get().parse(s);
//                return Optional.of(sdfT.get().format(startTimeOfBill));
                LocalDateTime startTimeOfBill = LocalDateTime.parse(s, dtfS.get());
                String formattedDate = startTimeOfBill.format(dtfT.get());
                return Optional.of(formattedDate);
            }
        } catch (Exception e) {
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
                if (number.length() < 10) {
                    number = "966" + number;
                }
            }
            if (number.length() < 10) {
                number = "966" + number;
            }
            return number;
        }
        return "";
    }
}
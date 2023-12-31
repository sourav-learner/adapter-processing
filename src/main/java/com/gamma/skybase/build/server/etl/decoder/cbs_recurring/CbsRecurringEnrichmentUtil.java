package com.gamma.skybase.build.server.etl.decoder.cbs_recurring;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsRecurringEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;
    private CbsRecurringEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsRecurringEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsRecurringEnrichmentUtil(record);
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

    String chargingPartyType;

    public Optional<String> getChargingPartyType() {
        chargingPartyType = getValue("ChargingPartyType");
        if (chargingPartyType != null) {
            switch (chargingPartyType) {
                case "0":
                    chargingPartyType = "SUBSCRIBER";
                    break;
                case "1":
                    chargingPartyType = "CUSTOMER";
                    break;
                case "2":
                    chargingPartyType = "SUBSCRIBER GROUP";
                    break;
                default:
                    chargingPartyType = "-99";
                    break;
            }
        }
        if (chargingPartyType != null)
            return Optional.of(chargingPartyType);
        return Optional.empty();
    }

    String cycleType;

    public Optional<String> getCycleType() {
        cycleType = getValue("CycleType");
        if (cycleType != null) {
            switch (cycleType) {
                case "1":
                    cycleType = "BILL CYCLE";
                    break;
                case "2":
                    cycleType = "DAYS";
                    break;
                case "3":
                    cycleType = "WEEKS";
                    break;
                case "4":
                    cycleType = "MONTHS";
                    break;
                default:
                    cycleType = "-99";
                    break;
            }
        }
        if (cycleType != null)
            return Optional.of(cycleType);
        return Optional.empty();
    }

    String payType , servedType;

    public Optional<String> getServedType() {
        payType = getValue("PayType");

        if (payType != null) {
            switch (payType) {
                case "0":
                    servedType = "PREPAID";
                    break;
                case "1":
                    servedType = "POSTPAID";
                    break;
                case "2":
                    servedType = "HYBRID";
                    break;
                default:
                    servedType = "UNKNOWN";
                    break;
            }
        }
        if (servedType != null)
            return Optional.of(servedType);
        return Optional.empty();
    }

    String orderStatus;

    public Optional<String> getOrderStatus() {
        orderStatus = getValue("OrderStatus");

        if (orderStatus != null) {
            switch (orderStatus) {
                case "0":
                    orderStatus = "NORMAL";
                    break;
                case "1":
                    orderStatus = "SUSPENDED";
                    break;
                case "2":
                    orderStatus = "INITIAL";
                    break;
                case "3":
                    orderStatus = "EXPIRED";
                    break;
                default:
                    orderStatus = "-99";
                    break;
            }
        }
        if (orderStatus != null)
            return Optional.of(orderStatus);
        return Optional.empty();
    }

    String chargePartyIndicator;

    public Optional<String> getChargePartyIndicator() {
        chargePartyIndicator = getValue("ChargePartyIndicator");

        if (chargePartyIndicator != null) {
            switch (chargePartyIndicator) {
                case "1":
                    chargePartyIndicator = "CALLING PARTY";
                    break;
                case "2":
                    chargePartyIndicator = "CALLEDPARTY";
                    break;
                case "3":
                    chargePartyIndicator = "THIRDPARTY";
                    break;
                case "4":
                    chargePartyIndicator = "PARENT|CHILD CARD SUBS";
                    break;
                case "5":
                    chargePartyIndicator = "CALLINGPARTYANDCORPORATE";
                    break;
                default:
                    chargePartyIndicator = "-99";
                    break;
            }
        }
        if (chargePartyIndicator != null)
            return Optional.of(chargePartyIndicator);
        return Optional.empty();
    }
}
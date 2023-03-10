package com.gamma.skybase.build.server.etl.decoder.crm_sim;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CrmSimEnrichmentUtil {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LinkedHashMap<String, Object> rec;

    private CrmSimEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CrmSimEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CrmSimEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    public Optional<String> getCreateDate() {
        LocalDateTime createDate;
        String createDate1;

        createDate1 = getValue("CREATE_DATE");
        if (createDate1 != null) {
            createDate = LocalDateTime.parse(createDate1, dtf2);
            return Optional.of(dtf.format(createDate));
        }
        return Optional.empty();
    }

    public Optional<String> getValidDate() {
        LocalDateTime validDate;
        String validDate1;

        validDate1 = getValue("VALID_DATE");
        if (validDate1 != null) {
            validDate = LocalDateTime.parse(validDate1, dtf2);
            return Optional.of(dtf.format(validDate));
        }
        return Optional.empty();
    }

    public Optional<String> getInvalidDate() {
        LocalDateTime inValidDate;
        String invalidDate1;

        invalidDate1 = getValue("INVALID_DATE");
        if (invalidDate1 != null) {
            inValidDate = LocalDateTime.parse(invalidDate1, dtf2);
            return Optional.of(dtf.format(inValidDate));
        }
        return Optional.empty();
    }

    public Optional<String> getOperDate() {
        LocalDateTime operDate;
        String operDate1;

        operDate1 = getValue("OPER_DATE");
        if (operDate1 != null) {
            operDate = LocalDateTime.parse(operDate1, dtf2);
            return Optional.of(dtf.format(operDate));
        }
        return Optional.empty();
    }


    String isBind;

    public Optional<String> getIsBind() {
        isBind = getValue("IS_BIND");
        if (isBind != null) {
            switch (isBind) {
                case "0":
                    isBind = "UNBOUND";
                    break;
                case "1":
                    isBind = "BOUND";
                    break;
                default:
                    isBind = "-99";
                    break;
            }
        }
        if (isBind != null)
            return Optional.of(isBind);
        return Optional.empty();
    }

    String isLocked;

    public Optional<String> getIsLocked() {
        isLocked = getValue("IS_LOCKED");
        if (isLocked != null) {
            switch (isLocked) {
                case "0":
                    isLocked = "UNLOCKED";
                    break;
                case "1":
                    isLocked = "LOCKED";
                    break;
                default:
                    isLocked = "-99";
                    break;
            }
        }
        if (isLocked != null)
            return Optional.of(isLocked);
        return Optional.empty();
    }

    String isRecycled;

    public Optional<String> getIsRecycled() {
        isRecycled = getValue("IS_LOCKED");
        if (isRecycled != null) {
            switch (isRecycled) {
                case "0":
                    isRecycled = "NOT RECYCLED";
                    break;
                case "1":
                    isRecycled = "RECYCLED";
                    break;
                default:
                    isRecycled = "-99";
                    break;
            }
        }
        if (isRecycled != null)
            return Optional.of(isRecycled);
        return Optional.empty();
    }


    String orderStatus;

    public Optional<String> getOrderStatus() {
        orderStatus = getValue("ORDER_STATUS");
        if (orderStatus != null) {
            switch (orderStatus) {
                case "0":
                    orderStatus = "CREATED";
                    break;
                case "1":
                    orderStatus = "RECEIVED";
                    break;
                case "2":
                    orderStatus = "READY";
                    break;
                case "3":
                    orderStatus = "RECEIVE";
                    break;
                case "4":
                    orderStatus = "SEND HKP File";
                    break;
                case "5":
                    orderStatus = "SEND HKP File Successful";
                    break;
                case "6":
                    orderStatus = "SEND failed";
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

    public Optional<String> getEventDate(){
        String fileName;
        fileName = getValue("fileName");
        String eventDate = null;
        Pattern pattern = Pattern.compile("^[A-Za-z0-9_]+_[A-Za-z0-9_]+_([0-9]{8})\\.[A-Za-z]+$");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            eventDate = matcher.group(1);
        }
        return Optional.of(eventDate);
    }
}

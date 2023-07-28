package com.gamma.skybase.build.server.etl.decoder.uvc_manlog;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class UvcManlogEnrichmentUtil{

    LinkedHashMap<String, Object> rec;
    public UvcManlogEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static UvcManlogEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new UvcManlogEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String faceValue;
    public Optional<String> getFaceValue() {
        faceValue = getValue("FACEVALUE");
        faceValue = String.valueOf(Integer.parseInt(faceValue) / 100);
        if (faceValue!= null){
            return Optional.of(faceValue);
        }
        return Optional.empty();
    }

    public Optional<String> getCardStateDate() {
        String cardStartDate;
        String cardStartDate1;

        cardStartDate1 = getValue("CARDSTARTDATE");
        if (cardStartDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(cardStartDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                cardStartDate = outputFormat.format(date);
                return Optional.of(cardStartDate);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    public Optional<String> getCardStopDate() {
        String cardStopDate;
        String cardStopDate1;

        cardStopDate1 = getValue("CARDSTOPDATE");
        if (cardStopDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(cardStopDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                cardStopDate = outputFormat.format(date);
                return Optional.of(cardStopDate);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String servedMSISDN;
    public Optional<String> getServedMSISDN() {
        String msisdn;
        msisdn = getValue("RECHARGENUMBER");
        String msisdn1 = normalizeMSISDN(msisdn);
        if (msisdn1 != null)
            servedMSISDN = msisdn1;
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String normalizeMSISDN(String number) {
        if (number != null){
            if (!number.startsWith("966")) {
                number = "966" + number;
            }
            else{
                return number;
            }
            return number;
        }
        return "";
    }

    public Optional<String> getTradeTime() {
        String tradeTime;
        String tradeTime1;

        tradeTime1 = getValue("TRADETIME");
        if (tradeTime1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = inputFormat.parse(tradeTime1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                tradeTime = outputFormat.format(date);
                return Optional.of(tradeTime);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String tradeType;

    public Optional<String> getExtractFirstDigit() {
        tradeType = getValue("TRADETYPE");
        if (!tradeType.isEmpty() && Character.isDigit(tradeType.charAt(0))) {
            char firstDigit = tradeType.charAt(0);
            return Optional.of(String.valueOf(firstDigit));

        }
        return Optional.empty();
    }

    public Optional<String> getExtractSecondDigit() {
        tradeType = getValue("TRADETYPE");
        if (tradeType.length() >= 2 && Character.isDigit(tradeType.charAt(1))) {
            char secondDigit = tradeType.charAt(1);
            return Optional.of(String.valueOf(secondDigit));
        }
        return Optional.empty();
    }

    public Optional<String> getExtractSixthDigit() {
        tradeType = getValue("TRADETYPE");
        if (tradeType.length() >= 6 && Character.isDigit(tradeType.charAt(5))) {
            char sixthDigit = tradeType.charAt(5);
            switch (sixthDigit) {
                case '0':
                    return Optional.of("password");
                case '1':
                    return Optional.of("Serial Number");
                default:
                    return Optional.of("unknown");
            }
        }
        return Optional.empty();
    }

    public Optional<String> getExtractSeventhDigit() {
        tradeType = getValue("TRADETYPE");
        if (tradeType.length() >= 7 && Character.isDigit(tradeType.charAt(6))) {
            char seventhDigit = tradeType.charAt(6);
            return Optional.of(String.valueOf(seventhDigit));
        }
        return Optional.empty();
    }

    public Optional<String> getExtractEighthDigit() {
        tradeType = getValue("TRADETYPE");
        if (tradeType.length() >= 8 && Character.isDigit(tradeType.charAt(7))) {
            char eighthDigit = tradeType.charAt(7);
            switch (eighthDigit) {
                case '0':
                    return Optional.of("Empty");
                case '1':
                    return Optional.of("prepaid");
                case '2':
                    return Optional.of("Postpaid");
                case '3':
                    return Optional.of("hybrid");
                case '4':
                    return Optional.of("international");
                case '5':
                    return Optional.of("others");
                default:
                    return Optional.of("unknown");
            }
        }
        return Optional.empty();
    }

    String errorTypeDesc;
    public Optional<String> getErrorTypeDesc() {
        errorTypeDesc = getValue("ERRORTYPE");
        if (errorTypeDesc != null) {
            switch (errorTypeDesc) {
                case "0":
                    errorTypeDesc = "Recharge Succeeded";
                    break;
                case "1":
                    errorTypeDesc = "Voucher expired";
                    break;
                case "11":
                    errorTypeDesc = "Voucher doesn’t exist";
                    break;
                case "12":
                    errorTypeDesc = "Voucher locked";
                    break;
                default:
                    errorTypeDesc = "unknown";
                    break;
            }
        }
        if (errorTypeDesc != null)
            return Optional.of(errorTypeDesc);
        return Optional.empty();
    }

    String rechargeType;
    public Optional<String> getRechargeType() {
        rechargeType = getValue("SEQUENCE");
        if (rechargeType.length() == 15) {
            rechargeType = "Physical Recharge";
        } else if (rechargeType.length() == 14) {
            rechargeType = "Electronic Recharge";
        }
        else {
            rechargeType = "unknown";
        }
        if (rechargeType != null)
            return Optional.of(rechargeType);
        return Optional.empty();
    }

    public Optional<String> getEventDate() {
        String timeStamp ;
        timeStamp = getValue("TRADETIME");
        DateTimeFormatter inputFormatter1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        try {
            LocalDateTime dateTime = LocalDateTime.parse(timeStamp, inputFormatter1);

            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String eventDate = dateTime.format(outputFormatter);

            return Optional.of(eventDate);
        } catch (DateTimeParseException e) {
//            e.printStackTrace();
        }
        return Optional.empty();
    }
}
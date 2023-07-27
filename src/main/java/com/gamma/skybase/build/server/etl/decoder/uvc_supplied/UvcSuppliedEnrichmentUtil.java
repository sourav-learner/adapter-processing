package com.gamma.skybase.build.server.etl.decoder.uvc_supplied;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class UvcSuppliedEnrichmentUtil {
    LinkedHashMap<String, Object> rec;

    private UvcSuppliedEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static UvcSuppliedEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new UvcSuppliedEnrichmentUtil(record);
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
        if (faceValue != null) {
            return Optional.of(faceValue);
        }
        return Optional.empty();
    }

    String createDate;
    String createDate1;

    public Optional<String> getCardStateDate() {
        createDate1 = getValue("CARDSTARTDATE");
        if (createDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(createDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                createDate = outputFormat.format(date);
                return Optional.of(createDate);
            }
            catch (ParseException e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String endDate1, endDate;

    public Optional<String> getCardStopDate() {
        endDate1 = getValue("CARDSTOPDATE");
        if (endDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(endDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                endDate = outputFormat.format(date);
                return Optional.of(endDate);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String tradeTime1;

    public Optional<String> getTradeTime() {
        tradeTime1 = getValue("TRADETIME");
        if (tradeTime1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = inputFormat.parse(tradeTime1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                String tradeTime = outputFormat.format(date);

                return Optional.of(tradeTime);

            } catch (Exception e) {
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String servedMSISDN;

    public Optional<String> getServedMSISDN() {
        String msisdn;
        msisdn = getValue("CHARGENUMBER");
        String msisdn1 = normalizeMSISDN(msisdn);
        if (msisdn1 != null)
            servedMSISDN = msisdn1;
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String normalizeMSISDN(String number) {
        if (number != null){
            if (!number.startsWith("966") || !number.startsWith("MCC")) {
                number = "966" + number;
            }
            else{
                return number;
            }
            return number;
        }
        return "";
    }

    String cardStopDateBak1, cardStopDateBak;

    public Optional<String> getCardStopDateBak() {
        cardStopDateBak1 = getValue("CARDSTOPDATEBAK");
        if (cardStopDateBak1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(cardStopDateBak1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                cardStopDateBak = outputFormat.format(date);
                return Optional.of(cardStopDateBak);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String authAvailableTime1, authAvailableTime;
    public Optional<String> getAuthAvailableTime() {
        authAvailableTime1 = getValue("AUTHAVALIBLETIME");
        if (authAvailableTime1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(authAvailableTime1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                authAvailableTime = outputFormat.format(date);
                return Optional.of(authAvailableTime);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String createNewDate1, createNewDate;
    public Optional<String> getCreateDate() {

        createNewDate1 = getValue("CREATEDATE");
        if (createNewDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(createNewDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd 00:00:00");
                createNewDate = outputFormat.format(date);
                return Optional.of(createNewDate);
            }
            catch (Exception e){
//                e.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String channelType;

    public Optional<String> getChannelType() {
        channelType = getValue("CHANNELTYPE");
        if (channelType != null) {
            switch (channelType) {
                case "0":
                    channelType = "physical channel";
                    break;
                case "1":
                    channelType = "E-PIN Channel";
                    break;
                default:
                    hotCardFlagDesc = "unknown";
                    break;
            }
        }
        if (channelType != null)
            return Optional.of(channelType);
        return Optional.empty();
    }

    String oprTypeDesc;

    public Optional<String> getOprTypeDesc() {
        oprTypeDesc = getValue("OPRTYPE");
        if (oprTypeDesc != null) {
            switch (oprTypeDesc) {
                case "2":
                    oprTypeDesc = "Load";
                    break;
                case "3":
                    oprTypeDesc = "Issue";
                    break;
                case "4":
                    oprTypeDesc = "Activate";
                    break;
                case "5":
                    oprTypeDesc = "Deactivate";
                    break;
                case "6":
                    oprTypeDesc = "Lock";
                    break;
                case "7":
                    oprTypeDesc = "Unlock";
                    break;
                case "9":
                    oprTypeDesc = "Prolong Validity Period";
                    break;
                case "c":
                    oprTypeDesc = "Permanent lock";
                    break;
                case "e":
                    oprTypeDesc = "Recharge by Password";
                    break;
                case "f":
                    oprTypeDesc = "Recharge by Sequence";
                    break;
                default:
                    oprTypeDesc = "unknown";
                    break;
            }
        }
        if (oprTypeDesc != null)
            return Optional.of(oprTypeDesc);
        return Optional.empty();
    }

    String useStateDesc;

    public Optional<String> getUseStateDesc() {
        useStateDesc = getValue("USESTATE");
        if (useStateDesc != null) {
            switch (useStateDesc) {
                case "0":
                    useStateDesc = "idle";
                    break;
                case "1":
                    useStateDesc = "in use";
                    break;
                case "2":
                    useStateDesc = "used";
                    break;
                case "3":
                    useStateDesc = "Locked";
                    break;
                case "4":
                    useStateDesc = "used up";
                    break;
                default:
                    useStateDesc = "unknown";
                    break;
            }
        }
        if (useStateDesc != null)
            return Optional.of(useStateDesc);
        return Optional.empty();
    }

    String hotCardFlagDesc;

    public Optional<String> getHotCardFlagDesc() {
        hotCardFlagDesc = getValue("HOTCARDFLAG");
        if (hotCardFlagDesc != null) {
            switch (hotCardFlagDesc) {
                case "0":
                    hotCardFlagDesc = "voucher not used for recharge but activated";
                    break;
                case "1":
                    hotCardFlagDesc = "recharge opertaion  complete";
                    break;
                case "2":
                    hotCardFlagDesc = "vouchere used for recharge";
                    break;
                case "3":
                    hotCardFlagDesc = "voucher loaded";
                    break;
                case "4":
                    hotCardFlagDesc = "voucher locked";
                    break;
                case "5":
                    hotCardFlagDesc = "voucher issued";
                    break;
                case "6":
                    hotCardFlagDesc = "voucher locked permanently";
                    break;
                default:
                    hotCardFlagDesc = "unknown";
                    break;
            }
        }
        if (hotCardFlagDesc != null)
            return Optional.of(hotCardFlagDesc);
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
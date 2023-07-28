package com.gamma.skybase.build.server.etl.decoder.uvc_supply;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class UvcsupplyEnrichmentUtil {

    LinkedHashMap<String, Object> rec;
    private UvcsupplyEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static UvcsupplyEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new UvcsupplyEnrichmentUtil(record);
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
            } catch (Exception e) {
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
            } catch (Exception e) {
//                e.printStackTrace();
            }
        }
        return Optional.empty();
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
        if (number != null) {
            if (!number.startsWith("966")) {
                number = "966" + number;
            } else {
                return number;
            }
            return number;
        }
        return "";
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
        return Optional.ofNullable(oprTypeDesc);
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
        } else {
            rechargeType = "unknown";
        }
        if (rechargeType != null)
            return Optional.of(rechargeType);
        return Optional.empty();
    }

    public Optional<String> getEventDate() {
        String eventDate;
        String eventDate1;

        eventDate1 = getValue("CARDSTARTDATE");
        if (eventDate1 != null) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = inputFormat.parse(eventDate1);

                SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");
                eventDate = outputFormat.format(date);
                return Optional.of(eventDate);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Optional.empty();
    }
}
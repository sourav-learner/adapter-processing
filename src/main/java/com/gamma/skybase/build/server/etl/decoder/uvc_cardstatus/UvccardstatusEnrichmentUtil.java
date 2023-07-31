package com.gamma.skybase.build.server.etl.decoder.uvc_cardstatus;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UvccardstatusEnrichmentUtil {

    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    LinkedHashMap<String, Object> rec;

    public UvccardstatusEnrichmentUtil(LinkedHashMap<String, Object> record) {
       rec = record;
    }

    public static UvccardstatusEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new UvccardstatusEnrichmentUtil(record);
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

    public Optional<String> getEventDate(){
        String fileName ;
        fileName = getValue("fileName");
        Pattern pattern = Pattern.compile("\\d{8}");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            String eventDate1 = matcher.group();
            if (eventDate1 != null) {
                return Optional.of(eventDate1);
            }
        }
        return Optional.empty();
    }

    public Optional<String> getXdrDate() {
        String fileName;
        fileName = getValue("fileName");
        Pattern pattern = Pattern.compile("\\d{8}");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            String dateString = matcher.group();
            try {
                if (dateString != null) {
                    String xdrDate = sdfT.get().format(fullDate.get().parse(dateString));
                    return Optional.of(xdrDate);
                }
            } catch (ParseException e) {
            }
        }
        return Optional.empty();
    }
}
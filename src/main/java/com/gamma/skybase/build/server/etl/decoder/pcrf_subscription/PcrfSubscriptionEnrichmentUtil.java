package com.gamma.skybase.build.server.etl.decoder.pcrf_subscription;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PcrfSubscriptionEnrichmentUtil extends LebaraUtil {

    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    public PcrfSubscriptionEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static PcrfSubscriptionEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new PcrfSubscriptionEnrichmentUtil(record);
    }

    Date xdrDate = null;

    public Optional<String> getXdrDate(){
        String fileName ;
        fileName = getValue("fileName");
        Pattern pattern = Pattern.compile("\\d{14}");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            String dateString = matcher.group();
            try {
                if (dateString != null) {
                    xdrDate = sdfS.get().parse(dateString);
                    return Optional.of(sdfT.get().format(xdrDate));
                }
            } catch (ParseException e) {// Ignore invalid Date format
            }
        }
        return Optional.empty();
    }

    public Optional<String> getEventDate(){
        String eventDate = null;
        Date eventDate1;
        eventDate1 = xdrDate;
        if (eventDate1 != null) {
            eventDate = fullDate.get().format(eventDate1);
            return Optional.of(eventDate);
        }
        return Optional.empty();
    }

}
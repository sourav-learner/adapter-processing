package com.gamma.skybase.build.server.etl.decoder.ota_mobile_info;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class OtaMobileInfoEnrichmentUtil extends LebaraUtil{
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private OtaMobileInfoEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static OtaMobileInfoEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new OtaMobileInfoEnrichmentUtil(record);
    }

    Date eventDate;
    String genFullDate;

    public Optional<String> getXdrDate() {
        String timeStamp ;
        timeStamp = getValue("TIMESTAMP");
        try {
            if (timeStamp != null) {
                eventDate = sdfS.get().parse(timeStamp);
                genFullDate = fullDate.get().format(eventDate);
                return Optional.of(sdfT.get().format(eventDate));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }
}
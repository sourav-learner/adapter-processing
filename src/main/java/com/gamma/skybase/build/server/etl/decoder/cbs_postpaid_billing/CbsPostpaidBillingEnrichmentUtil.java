package com.gamma.skybase.build.server.etl.decoder.cbs_postpaid_billing;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsPostpaidBillingEnrichmentUtil {

    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private CbsPostpaidBillingEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsPostpaidBillingEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsPostpaidBillingEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    Date callStartDate;
    String genFullDate;

    public Optional<String> getStartDate(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callStartDate = sdfS.get().parse(s);
                genFullDate = fullDate.get().format(callStartDate);
                return Optional.of(sdfT.get().format(callStartDate));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }
}
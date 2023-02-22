package com.gamma.skybase.build.server.etl.decoder;

import com.gamma.telco.OpcoBusinessTransformation;

import java.util.LinkedHashMap;

import static com.gamma.telco.utility.TelcoBusinessTransformation.cache;

public class LebaraUtil {
    protected final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    LinkedHashMap<String, Object> rec;

    public LebaraUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    public static ReferenceDimRoamingPartnerInfo getDimRoamingPartnerInfo(String tadigCode) {
        try {
            Object x = cache.getRecord("DIM_ROAMING_PARTNER_INFO_CACHE", tadigCode);
            if (x != null)
                return (ReferenceDimRoamingPartnerInfo) x;
        } catch (Exception ignore) {
        }
        return null;
    }

    public int isPrepaid(String msisdn) {
        String value;

        ReferenceDimSuscriberCRMInf subInfo = (ReferenceDimSuscriberCRMInf) cache.getRecord("DIM_CRM_INF_SUBSCRIBER_ALL_LOOKUP_CACHE", msisdn);
        if (subInfo != null) {
            value = subInfo.getPrepaidFlag();
            if (value != null)
                try {
                    int val =Integer.parseInt(value);
                    return val;
                } catch (Exception ignore) {
                }
        }
        return -1;// 0 for Prepaid, 1 for postpaid, -1 unknown
    }
}
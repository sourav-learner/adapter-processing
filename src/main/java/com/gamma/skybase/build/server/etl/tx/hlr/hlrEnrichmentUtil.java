package com.gamma.skybase.build.server.etl.tx.hlr;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Optional;

public class hlrEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private hlrEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static hlrEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new hlrEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }
    String networkAccessMode , nam;

    public Optional<String> getNetworkAccessMode() {
        nam = getValue("NAM");
        if (nam != null) {
            switch (nam) {
                case "0":
                    networkAccessMode = "PS+CS";
                    break;
                case "1":
                    networkAccessMode = "MSC-CS Mode";
                    break;
                case "2":
                    networkAccessMode = "SGSN_PC Mode";
                    break;
                default:
                    networkAccessMode = "-99";
                    break;
            }
        }
        if (networkAccessMode != null)
            return Optional.of(networkAccessMode);
        return Optional.empty();
    }

    String odbIncomingCall , odbic;

    public Optional<String> getOdbIncomingCall() {
        odbic = getValue("ODBIC");
        if (odbic != null) {
            switch (odbic) {
                case "0":
                    odbIncomingCall = "NO_BAR";
                    break;
                case "1":
                    odbIncomingCall = "IBARRED";
                    break;
                case "2":
                    odbIncomingCall = "BARRED_WHILE_ROAMING";
                    break;
                default:
                    break;
            }
        }
        if (odbIncomingCall != null)
            return Optional.of(odbIncomingCall);
        return Optional.empty();
    }
    String odboc , odbOutgoingCall;
    public Optional<String> getOdbOutgoingCall() {
        odboc = getValue("ODBOC");
        if (odboc != null) {
            switch (odboc) {
                case "0":
                    odbOutgoingCall = "NO_BAR";
                    break;
                case "1":
                    odbOutgoingCall = "BARRED";
                    break;
                case "2":
                    odbOutgoingCall = "OUTGOING_INTERNATIONAL_BARRED";
                    break;
                case "3":
                    odbOutgoingCall = "OUTGOING_INTERNATIONAL_EXCEPT_TO_HOME_BARRED";
                    break;
                case "4":
                    odbOutgoingCall = "OUTGOING_BARRED_WHILE_ROAMING";
                    break;
                default:
                    odbOutgoingCall = "-99";
                    break;
            }
        }
        if (odbOutgoingCall != null)
            return Optional.of(odbOutgoingCall);
        return Optional.empty();
    }
}
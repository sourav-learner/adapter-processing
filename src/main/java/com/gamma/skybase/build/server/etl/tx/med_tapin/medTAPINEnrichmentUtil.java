package com.gamma.skybase.build.server.etl.tx.med_tapin;

import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimCRMSubscriber;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import com.gamma.telco.utility.reference.ReferenceDimTadigLookup;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.gamma.telco.utility.TelcoBusinessTransformation.cache;

public class medTAPINEnrichmentUtil {
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    LinkedHashMap<String, Object> rec;

    private medTAPINEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static medTAPINEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new medTAPINEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    LocalDateTime eventStartTime;
    String genFullDate, callTimeStamp;

    public Optional<String> getEventStartTime(String field) {
        callTimeStamp = getValue(field);
        try {
            if (callTimeStamp != null) {
                eventStartTime = LocalDateTime.parse(callTimeStamp, dtf2);
                genFullDate = eventStartTime.toLocalDate().format(dtf1);
                return Optional.of(dtf.format(eventStartTime));
            }
        } catch (Exception e) {
        }
        return Optional.empty();
    }

    String servedMSISDN, aPartyNumber, callTerminatingFlag, bPartyNumber;

    public Optional<String> getServedMSISDN() {
        callTerminatingFlag = getValue("CALL_TERMINATING_FLAG");
        aPartyNumber = getValue("A_PARTY_NUMBER");
        bPartyNumber = getValue("B_PARTY_NUMBER");
        if (callTerminatingFlag != null)
            switch (callTerminatingFlag) {
                case "0":
                    servedMSISDN = aPartyNumber;
                    break;
                case "1":
                    servedMSISDN = bPartyNumber;
                    break;
                default:
                    break;
            }
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String otherMSISDN;

    public Optional<String> getOtherMSISDN() {
        if (callTerminatingFlag != null)
            switch (callTerminatingFlag) {
                case "0":
                    otherMSISDN = bPartyNumber;
                    break;
                case "1":
                    otherMSISDN = aPartyNumber;
                    break;
                default:
                    break;
            }
        if (otherMSISDN != null)
            return Optional.of(otherMSISDN);
        return Optional.empty();
    }

    LocalDateTime tapRatedDate;

    public Optional<String> getTapRatedDate(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                tapRatedDate = LocalDateTime.parse(s, dtf2);
                return Optional.of(dtf.format(tapRatedDate));
            }
        } catch (Exception e) {
        }
        return Optional.empty();
    }

    String volumeOutgoing;

    public Optional<String> getVolumeOutgoing() {
        String typeService = getValue("TYPE_OF_SERVICE");
        if (typeService != null)
            switch (typeService.toUpperCase()) {
                case "C":
                case "S":
                    volumeOutgoing = "NA";
                    break;
                default:
                    break;
            }
        if (volumeOutgoing != null)
            return Optional.of(volumeOutgoing);
        return Optional.empty();
    }

    String totalVolume;

    public Optional<String> getTotalVolume() {
        String typeService = getValue("TYPE_OF_SERVICE");
        if (typeService != null)
            switch (typeService) {
                case "C":
                case "S":
                    totalVolume = "NA";
                    break;
                default:
                    break;
            }
        if (totalVolume != null)
            return Optional.of(totalVolume);
        return Optional.empty();
    }

    String volumeIncoming;

    public Optional<String> getVolumeIncoming() {
        String typeService = getValue("TYPE_OF_SERVICE");
        if (typeService != null)
            switch (typeService) {
                case "C":
                case "S":
                    volumeIncoming = "NA";
                    break;
                default:
                    break;
            }
        if (volumeIncoming != null)
            return Optional.of(volumeIncoming);
        return Optional.empty();
    }

    String srvTypeKey, msrn;

    public int isPrepaid(String servedMSISDN) {
        String value;
        ReferenceDimCRMSubscriber subInfo = (ReferenceDimCRMSubscriber) cache.getRecord("DIM_CRM_INF_SUBSCRIBER_ALL", servedMSISDN);
        //todo fix it
        if (subInfo != null) {
            value = subInfo.getServedMsisdn();
            if (value != null) {
//                return Integer.parseInt(subInfo.getPrepaidFlag());//TODO
                return 0;
            }
        }
        return 1;
    }

    public Optional<String> getSrvTypeKey() {
        msrn = getValue("MSRN");
        boolean msrnFlag;
        if (msrn != null) {
            msrnFlag = msrn.startsWith("966");
            int flag = isPrepaid(servedMSISDN);
            switch (flag) {
                case 0:
                    if (msrnFlag) {
                        srvTypeKey = "1";
                    } else {
                        srvTypeKey = "5";
                    }
                    break;
                case 1:
                    if (msrnFlag) {
                        srvTypeKey = "2";
                    } else {
                        srvTypeKey = "6";
                    }
                    break;
                case 3:
                    if (msrnFlag) {
                        srvTypeKey = "7";
                    } else {
                        srvTypeKey = "8";
                    }
                    break;
                default:
                    srvTypeKey = "-99";
                    break;
            }

            if (srvTypeKey != null)
                return Optional.of(srvTypeKey);
        }
        return Optional.empty();
    }

    String eventDirectionKey;

    public Optional<String> getEventDirectionKey() {
        if (callTerminatingFlag != null)
            switch (callTerminatingFlag) {
                case "0":
                    eventDirectionKey = "1";
                    break;
                case "1":
                    eventDirectionKey = "2";
                    break;
                default:
                    break;
            }

        if (eventDirectionKey != null)
            return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    String chrgUnitIdKey;

    public Optional<String> getChrgUnitIdKey() {
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService != null)
            switch (typeOfService) {
                case "C":
                    chrgUnitIdKey = "3";
                    break;
                case "S":
                    chrgUnitIdKey = "0";
                    break;
                case "G":
                    chrgUnitIdKey = "7";
                    break;
                default:
                    break;
            }
        if (chrgUnitIdKey != null) {
            return Optional.of(chrgUnitIdKey);
        }
        return Optional.empty();
    }

    String eventTypeKey;

    public Optional<String> getEventTypeKey() {
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService != null)
        switch (typeOfService) {
            case "C":
                eventTypeKey = "1";
                break;
            case "S":
                eventTypeKey = "2";
                break;
            case "G":
                eventTypeKey = "4";
                break;
            default:
                break;
        }
        if (eventTypeKey != null)
            return Optional.of(eventTypeKey);
        return Optional.empty();
    }

    String duration = null;
    long billablePulse = 0L;

    public Optional<Long> getBillablePulse() {
        duration = getValue("DURATION");
        if (duration != null) {
            try {
                long v = Long.parseLong(duration);
                billablePulse = (long) Math.ceil((double) v / 60);
                return Optional.of(billablePulse);
            } catch (Exception ignore) {
            }
        }
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService.equals("G")) {
            billablePulse = -97;
        }

        return Optional.empty();
    }

    String volumeOut, uplinkVolume;

    public Optional<String> getGprsUplinkVolume() {
        volumeOut = getValue("VOLUME_OUTGOING");
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService != null) {
            switch (typeOfService) {
                case "G":
                    try {
                        Long v = Long.parseLong(volumeOut);
                        double d = v / 1024;
                        uplinkVolume = String.valueOf(d);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "C":
                case "S":
                    uplinkVolume = "NA";
                    break;
                default:
                    break;
            }
        }
        if (uplinkVolume != null) {
            return Optional.of(uplinkVolume);
        }
        return Optional.empty();
    }

    String volumeIn, downLinkVolume;

    public Optional<String> getGprsDownlinkVolume() {
        volumeIn = getValue("VOLUME_INCOMING");
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService != null) {
            switch (typeOfService) {
                case "G":
                    downLinkVolume = String.valueOf(Long.parseLong(volumeIn) / 1024);
                    break;
                case "C":
                case "S":
                    downLinkVolume = "NA";
                    break;
                default:
                    break;
            }
        }
        if (downLinkVolume != null) {
            return Optional.of(downLinkVolume);
        }
        return Optional.empty();
    }

    String totalVol, total;

    public Optional<String> getGprsTotalVolume() {
        totalVol = getValue("TOTAL_VOLUME");
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService != null) {
            switch (typeOfService) {
                case "G":
                    total = String.valueOf(Long.parseLong(totalVol) / 1024);
                    break;
                case "C":
                case "S":
                    total = "NA";
                    break;
                default:
                    break;
            }
        } else {
            total = volumeIn + volumeOut;
        }
        if (total != null) {
            return Optional.of(total);
        }
        return Optional.empty();
    }

    ReferenceDimDialDigit getDialedDigitSettings(String otherMSISDN) {
        return txLib.getDialedDigitSettings(otherMSISDN);
    }

    public Map<String, Object> getTAPINBasicInfo() {
        Map<String, Object> values = new HashMap<>();
        ReferenceDimTadigLookup partnerPlmnDetails = null;
        if (partnerPlmnDetails != null) {
            values.put("PARTNER_COUNTRY", partnerPlmnDetails.getCountry());
            values.put("PARTNER_OPER", partnerPlmnDetails.getOperator());
        }
        return values;
    }
}
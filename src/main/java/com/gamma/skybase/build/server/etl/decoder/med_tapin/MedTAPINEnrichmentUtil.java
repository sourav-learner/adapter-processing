package com.gamma.skybase.build.server.etl.decoder.med_tapin;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimRoamingPartnerInfo;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class MedTAPINEnrichmentUtil extends LebaraUtil {
    private final OpcoBusinessTransformation txLib = new OpcoBusinessTransformation();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    public MedTAPINEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static MedTAPINEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new MedTAPINEnrichmentUtil(record);
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
        } catch (Exception ignore) {
        }
        return Optional.empty();
    }

    public Optional<String> getEndTime() throws ParseException {
        String dur = getValue("DURATION");

        if (dur != null) {
            try {
                long i = Long.parseLong(dur);
                String dateTimeString = getValue("CALL_TIMESTAMP");
                if (dateTimeString != null) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    LocalDateTime startTime = LocalDateTime.parse(dateTimeString, formatter);
                    LocalDateTime endTime = startTime.plusSeconds(i);
                    DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
                    String eventEndTime = formatter1.format(endTime);
                    return Optional.of(eventEndTime);
                }
            } catch (Exception ignore) {
//                ignore.printStackTrace();
            }
        }
        return Optional.empty();
    }


    public Optional<String> getServedMSISDN() {
        String aPartyNumber, callTerminatingFlag, bPartyNumber;
        callTerminatingFlag = getValue("CALL_TERMINATING_FLAG");
        aPartyNumber = getValue("A_PARTY_NUMBER");
        bPartyNumber = getValue("B_PARTY_NUMBER");
        String servedMSISDN = null;
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
        String callTerminatingFlag = getValue("CALL_TERMINATING_FLAG");
        String aPartyNumber = getValue("A_PARTY_NUMBER");
        String bPartyNumber = getValue("B_PARTY_NUMBER");
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
//            e.printStackTrace();
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

    String srvTypeKey;

    public Optional<String> getSrvTypeKey(String msisdn) {
        int flag = isPrepaid(msisdn);
        switch (flag) {
            case 0:
                srvTypeKey = "5";
                break;
            case 1:
                srvTypeKey = "6";
                break;
            case 3:
                srvTypeKey = "8";
                break;
            default:
                srvTypeKey = "-97";
                break;
        }
        return Optional.of(srvTypeKey);
    }

    String eventDirectionKey;

    public Optional<String> getEventDirectionKey() {
        String callTerminatingFlag = getValue("CALL_TERMINATING_FLAG");
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


    public Optional<Long> getBillablePulse() {
        String typeOfService = getValue("TYPE_OF_SERVICE");
        if (typeOfService.equals("G"))
            return Optional.of(-97L);

        String duration = getValue("DURATION");
        if (duration != null) {
            try {
                long v = Long.parseLong(duration);
                long billablePulse = (long) Math.ceil((double) v / 60);
                return Optional.of(billablePulse);
            } catch (Exception ignore) {
            }
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
                        long v = Long.parseLong(volumeOut);
                        uplinkVolume = String.valueOf(v / 1024);
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

        String tadigCode = getValue("TADIG_CODE");
        ReferenceDimRoamingPartnerInfo roamingPartnerInfo = LebaraUtil.getDimRoamingPartnerInfo(tadigCode);
        Map<String, Object> values = new HashMap<>();
        if (roamingPartnerInfo != null) {
            values.put("PARTNER_COUNTRY", roamingPartnerInfo.getOperatorCountry());
            values.put("PARTNER_OPER", roamingPartnerInfo.getOperatorName());
        }
        return values;
    }
}
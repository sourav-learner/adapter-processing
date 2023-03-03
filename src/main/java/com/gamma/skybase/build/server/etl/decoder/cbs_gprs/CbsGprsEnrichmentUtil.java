package com.gamma.skybase.build.server.etl.decoder.cbs_gprs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CbsGprsEnrichmentUtil {
    final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    final ThreadLocal<SimpleDateFormat> fullDate = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> sdfS = ThreadLocal.withInitial(
            () -> new SimpleDateFormat("yyyyMMddHHmmss"));
    LinkedHashMap<String, Object> rec;

    private CbsGprsEnrichmentUtil(LinkedHashMap<String, Object> record) {
        rec = record;
    }

    public static CbsGprsEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CbsGprsEnrichmentUtil(record);
    }

    public String getValue(String field) {
        Object s = rec.get(field);
        if (s != null) {
            String s1 = s.toString().trim();
            if (!s1.equals("")) return s1;
        }
        return null;
    }

    String status;

    public Optional<String> getStatus() {
        status = getValue("STATUS");
        if (status != null) {
            switch (status) {
                case "A":
                    status = "VALID";
                    break;
                case "D":
                    status = "INVALID";
                    break;
                default:
                    status = "-99";
                    break;
            }
        }
        if (status != null)
            return Optional.of(status);
        return Optional.empty();
    }

    Date callStartTime;
    String genFullDate;

    public Optional<String> getStartTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callStartTime = sdfS.get().parse(s);
                genFullDate = fullDate.get().format(callStartTime);
                return Optional.of(sdfT.get().format(callStartTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    Date callEndTime;

    public Optional<String> getEndTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                callEndTime = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(callEndTime));
            }
        }
        catch (Exception e){
        }
        return Optional.empty();
    }

    String objType;

    public Optional<String> getObjType() {
        objType = getValue("OBJ_TYPE");
        if (objType != null) {
            switch (objType) {
                case "A":
                    objType = "ACC";
                    break;
                case "C":
                    objType = "CUSTOMER";
                    break;
                case "S":
                    objType = "SUBSCRIBER";
                    break;
                case "G":
                    objType = "GROUP";
                    break;
                default:
                    objType = "-99";
                    break;
            }
        }
        if (objType != null)
            return Optional.of(objType);
        return Optional.empty();
    }

    String resultCode;

    public Optional<String> getResultCode() {
        resultCode = getValue("RESULT_CODE");
        if (resultCode != null) {
            switch (resultCode) {
                case "0":
                    resultCode = "Success";
                    break;
                default:
                    resultCode = "Fail";
                    break;
            }
        }
        if (resultCode != null)
            return Optional.of(resultCode);
        return Optional.empty();
    }

    String eventDirectionKey;

    public Optional<String> getEventDirectionKey() {
        eventDirectionKey = getValue("ServiceFlow");
        if (eventDirectionKey != null) {
            switch (eventDirectionKey) {
                case "1":
                case "3":
                    eventDirectionKey = "1";
                    break;
                case "2":
                    eventDirectionKey = "2";
                    break;
            }
        }
        if (eventDirectionKey != null)
            return Optional.of(eventDirectionKey);
        return Optional.empty();
    }

    Date chargingTime;

    public Optional<String> getChargingTime(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                chargingTime = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(chargingTime));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    String upFlux;
    Long uploadVolume;


    public Optional<Long> getUploadVolume(String UpFlux) {
        upFlux = getValue(UpFlux);
        if (upFlux != null && !upFlux.isEmpty()) {
            try {
                uploadVolume = Long.parseLong(upFlux) / 1024;
                return Optional.of(uploadVolume);
            } catch (Exception ignored) {
                ignored.printStackTrace();
            }
        }
        return Optional.empty();
    }

    String downFlux;
    Long downloadVolume;

    public Optional<Long> getDownloadVolume(String DownFlux) {
        downFlux = getValue(DownFlux);
        if (downFlux != null) {
            try {
                downloadVolume = Long.valueOf(downFlux) / 1024;
                return Optional.of(downloadVolume);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    Long totalVolume;
    String totalFlux;

    public Optional<Long> getTotalVol(String TotalFlux) {
        totalFlux = getValue(TotalFlux);
        if (totalFlux != null) {
            try {
                totalVolume = Long.valueOf(totalFlux) / 1024;
                return Optional.of(totalVolume);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    String servedType, roamState, payType;

    public Optional<String> getServedType() {
        payType = getValue("PayType");
        roamState = getValue("RoamState");

        if (roamState != null) {
            switch (roamState) {
                case "3":
                    switch (payType) {
                        case "1":
                            servedType = "5";
                            break;
                        case "0":
                            servedType = "6";
                            break;
                        case "2":
                            servedType = "8";
                            break;
                    }
                    break;
                case "0":
                    switch (payType) {
                        case "0":
                            servedType = "2";
                            break;
                        case "1":
                            servedType = "1";
                            break;
                        case "2":
                            servedType = "7";
                            break;
                    }
                    break;
                default:
                    servedType = payType;
                    break;
            }
        }
        if (servedType != null)
            return Optional.of(servedType);
        return Optional.empty();
    }

    String onlineChargingFlag;

    public Optional<String> getOnlineChargingFlag() {
        onlineChargingFlag = getValue("OnlineChargingFlag");
        if (onlineChargingFlag != null) {
            switch (onlineChargingFlag) {
                case "1":
                    onlineChargingFlag = "Online";
                    break;
                case "2":
                    onlineChargingFlag = "offline";
                    break;
                default:
                    onlineChargingFlag = "-99";
                    break;
            }
        }

        if (onlineChargingFlag != null)
            return Optional.of(onlineChargingFlag);
        return Optional.empty();
    }

    Date startTimeOfBill;

    public Optional<String> getStartTimeOfBillCycle(String field) {
        String s = getValue(field);
        try {
            if (s != null) {
                startTimeOfBill = sdfS.get().parse(s);
                return Optional.of(sdfT.get().format(startTimeOfBill));
            }
        } catch (ParseException e) {// Ignore invalid Date format
        }
        return Optional.empty();
    }

    Double charge;

    public Optional<Double> getCharge(String DEBIT_AMOUNT) {
        String temp = getValue(DEBIT_AMOUNT);
        if (temp != null) {
            try {
                charge = Double.parseDouble(temp) / 1000;
                return Optional.of(charge);
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }
}
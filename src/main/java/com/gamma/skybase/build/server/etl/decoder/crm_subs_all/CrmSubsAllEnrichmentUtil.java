package com.gamma.skybase.build.server.etl.decoder.crm_subs_all;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CrmSubsAllEnrichmentUtil extends LebaraUtil{

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private CrmSubsAllEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static CrmSubsAllEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CrmSubsAllEnrichmentUtil(record);
    }

    String subType;

    public Optional<String> getSubType() {
        subType = getValue("SUB_TYPE");
        if (subType != null) {
            switch (subType) {
                case "0":
                    subType = "INDIVIDUAL";
                    break;
                case "1":
                    subType = "GROUP";
                    break;
                case "2":
                    subType = "VIRTUAL";
                    break;
                default:
                    subType = "-99";
                    break;
            }
        }

        if (subType != null)
            return Optional.of(subType);
        return Optional.empty();
    }

    String srvTypeKey;

    public Optional<String> getSrvTypeKey(String msisdn) {
        int flag = isPrepaid(msisdn);
        switch (flag) {
            case 0:
                srvTypeKey = "1";
                break;
            case 1:
                srvTypeKey = "2";
                break;
            case 3:
                srvTypeKey = "7";
                break;
            default:
                srvTypeKey = "-97";
                break;
        }
        return Optional.of(srvTypeKey);
    }

    String servedMSISDN;

    public Optional<String> getServedMSISDN() {
        String msisdn;
        msisdn = getValue("MSISDN");
        String msisdn1 = normalizeMSISDN(msisdn);
        if (msisdn1 != null)
            servedMSISDN = msisdn1;
        if (servedMSISDN != null)
            return Optional.of(servedMSISDN);
        return Optional.empty();
    }

    String normalizeMSISDN(String number) {
        if (number != null){
            if (number.startsWith("966")) {
                return number;
            }
            else{
                number = "966" + number;
            }
            return number;
        }
        return "";
    }

    public Optional<String> getSubLevel() {
        String subLevel = getValue("SUB_LEVEL");
        if (subLevel != null) {
            switch (subLevel) {
                case "5":
                    subLevel = "7 STAR";
                    break;
                case "10":
                    subLevel = "5 STAR";
                    break;
                case "15":
                    subLevel ="ECONOMY";
                    break;
                default:
                    subLevel = "-99";
                    break;
            }
        }
        if (subLevel != null)
            return Optional.of(subLevel);
        return Optional.empty();
    }

    public Optional<String> getDunFlag() {
        String dunFlag = getValue("DUN_FLAG");
        if (dunFlag != null) {
            switch (dunFlag) {
                case "0":
                    dunFlag = "CAN BE DUNNED AND BARRED";
                    break;
                case "1":
                    dunFlag = "CAN BE DUNNED BT NOT BARRED";
                    break;
                case "2":
                    dunFlag ="CANNOT BE DUNNER OR BARRED";
                    break;
                default:
                    dunFlag = "-99";
                    break;
            }
        }
        if (dunFlag != null)
            return Optional.of(dunFlag);
        return Optional.empty();
    }

    public Optional<String> getCreateDate() {
        LocalDateTime createDate;
        String createDate1;

        createDate1 = getValue("CREATE_DATE");
        if (createDate1 != null) {
            createDate = LocalDateTime.parse(createDate1, dtf2);
            return Optional.of(dtf.format(createDate));
        }
        return Optional.empty();
    }

    public Optional<String> getAgreementDate() {
        LocalDateTime agreementDate;
        String agreementDate1;

        agreementDate1 = getValue("AGREEMENT_DATE");
        if (agreementDate1 != null) {
            agreementDate = LocalDateTime.parse(agreementDate1, dtf2);
            return Optional.of(dtf.format(agreementDate));
        }
        return Optional.empty();
    }

    public Optional<String> getFirstActivationDate() {
        LocalDateTime firstActivationDate;
        String firstActivationDate1;

        firstActivationDate1 = getValue("FIRST_EFF_DATE");
        if (firstActivationDate1 != null) {
            firstActivationDate = LocalDateTime.parse(firstActivationDate1, dtf2);
            return Optional.of(dtf.format(firstActivationDate));
        }
        return Optional.empty();
    }

    public Optional<String> getEffDate() {
        LocalDateTime effDate;
        String effDate1;

        effDate1 = getValue("EFF_DATE");
        if (effDate1 != null) {
            effDate = LocalDateTime.parse(effDate1, dtf2);
            return Optional.of(dtf.format(effDate));
        }
        return Optional.empty();
    }

    public Optional<String> getExpDate() {
        LocalDateTime expDate;
        String expDate1;

        expDate1 = getValue("EXP_DATE");
        if (expDate1 != null) {
            expDate = LocalDateTime.parse(expDate1, dtf2);
            return Optional.of(dtf.format(expDate));
        }
        return Optional.empty();
    }


    public Optional<String> getModDate() {
        LocalDateTime modDate;
        String modDate1;

        modDate1 = getValue("MOD_DATE");
        if (modDate1 != null) {
            modDate = LocalDateTime.parse(modDate1, dtf2);
            return Optional.of(dtf.format(modDate));
        }
        return Optional.empty();
    }


    public Optional<String> getActiveDate() {
        LocalDateTime activeDate;
        String activeDate1;

        activeDate1 = getValue("ACTIVE_DATE");
        if (activeDate1 != null) {
            activeDate = LocalDateTime.parse(activeDate1, dtf2);
            return Optional.of(dtf.format(activeDate));
        }
        return Optional.empty();
    }

    public Optional<String> getLatestActiveDate() {
        LocalDateTime latestActiveDate;
        String latestActiveDate1;

        latestActiveDate1 = getValue("LATEST_ACTIVE_DATE");
        if (latestActiveDate1 != null) {
            latestActiveDate = LocalDateTime.parse(latestActiveDate1, dtf2);
            return Optional.of(dtf.format(latestActiveDate));
        }
        return Optional.empty();
    }

    public Optional<String> getEventDate(){
        String fileName;
        fileName = getValue("fileName");
        String eventDate = null;
        Pattern pattern = Pattern.compile("^[A-Za-z0-9_]+_[A-Za-z0-9_]+_([0-9]{8})\\.[A-Za-z]+$");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            eventDate = matcher.group(1);
        }
        return Optional.of(eventDate);
    }
}
package com.gamma.skybase.build.server.etl.decoder.crm_cust_all;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CrmCustAllEnrichmentUtil extends LebaraUtil{
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private CrmCustAllEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static CrmCustAllEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new CrmCustAllEnrichmentUtil(record);
    }

    public Optional<String> getCustTitle() {
        String custTitle1 = getValue("CUST_TITLE");
        String custTitle = null;
        if (custTitle1 != null) {
            switch (custTitle1) {
                case "1":
                    custTitle = "Mr.";
                    break;
                case "2":
                    custTitle = "Ms.";
                    break;
                case "3":
                    custTitle ="Mrs.";
                    break;
                default:
                    custTitle = "-99";
                    break;
            }
        }
        if (custTitle != null)
            return Optional.of(custTitle);
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

    public Optional<String> getServiceCategory() {
        String serviceCategory1 = getValue("SERVICE_CATEGORY");
        String serviceCategory = null;
        if (serviceCategory1 != null) {
            switch (serviceCategory1) {
                case "0":
                    serviceCategory = "PERSONAL DATE NOT CONFIRMED";
                    break;
                case "1":
                    serviceCategory = "PERSONAL DATE CONFIRMED";
                    break;
                default:
                    serviceCategory = "-99";
                    break;
            }
        }
        if (serviceCategory != null)
            return Optional.of(serviceCategory);
        return Optional.empty();
    }

    public Optional<String> getServiceLimitFlag() {
        String serviceLimitFlag1 = getValue("SERVICE_CATEGORY");
        String serviceLimitFlag = null;
        if (serviceLimitFlag1 != null) {
            switch (serviceLimitFlag1) {
                case "0":
                    serviceLimitFlag = "NOTIFY";
                    break;
                case "1":
                    serviceLimitFlag = "DOESN'T NOTIFY";
                    break;
                default:
                    serviceLimitFlag = "-99";
                    break;
            }
        }
        if (serviceLimitFlag != null)
            return Optional.of(serviceLimitFlag);
        return Optional.empty();
    }

    public Optional<String> getDocumentStatus() {
        String documentStatus1 = getValue("DOCUMENT_STATUS");
        String documentStatus = null;
        if (documentStatus1 != null) {
            switch (documentStatus1) {
                case "0":
                    documentStatus = "NOT COMPLETE";
                    break;
                case "1":
                    documentStatus = "INITIAL(RIGHT)";
                    break;
                case "2":
                    documentStatus = "CONFIRMED";
                    break;
                case "3":
                    documentStatus = "ERROR";
                    break;
                default:
                    documentStatus = "-99";
                    break;
            }
        }
        if (documentStatus != null)
            return Optional.of(documentStatus);
        return Optional.empty();
    }

    public Optional<String> getDocumentStatusTime() {
        LocalDateTime documentStatusTime;
        String documentStatusTime1;

        documentStatusTime1 = getValue("DOCUMENT_STATUS_TIME");
        if (documentStatusTime1 != null) {
            documentStatusTime = LocalDateTime.parse(documentStatusTime1, dtf2);
            return Optional.of(dtf.format(documentStatusTime));
        }
        return Optional.empty();
    }

    public Optional<String> getIsRegCust() {
        String isRegCust1 = getValue("IS_REG_CUST");
        String isRegCust = null;
        if (isRegCust1 != null) {
            switch (isRegCust1) {
                case "0":
                    isRegCust = "Holder Customer";
                    break;
                case "1":
                    isRegCust = "Registered Customer";
                    break;
                default:
                    isRegCust = "-99";
                    break;
            }
        }
        if (isRegCust != null)
            return Optional.of(isRegCust);
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
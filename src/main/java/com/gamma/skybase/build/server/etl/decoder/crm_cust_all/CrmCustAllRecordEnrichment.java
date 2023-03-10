package com.gamma.skybase.build.server.etl.decoder.crm_cust_all;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CrmCustAllRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CrmCustAllEnrichmentUtil tx = CrmCustAllEnrichmentUtil.of(record);

//        CUST_TITLE
        Optional<String> custTitle = tx.getCustTitle();
        custTitle.ifPresent(s -> record.put("CUSTTITLE", s));

//        CREATE_DATE
        Optional<String> createDate = tx.getCreateDate();
        createDate.ifPresent(s -> record.put("CREATEDATE", s));

//        EFF_DATE
        Optional<String> effDate = tx.getEffDate();
        effDate.ifPresent(s -> record.put("EFFDATE", s));

//        EXP_DATE
        Optional<String> expDate = tx.getExpDate();
        expDate.ifPresent(s -> record.put("EXPY_DATE", s));

//        MOD_DATE
        Optional<String> modDate = tx.getModDate();
        modDate.ifPresent(s -> record.put("MODDATE", s));

//        SERVICE_CATEGORY
        Optional<String> serviceCategory = tx.getServiceCategory();
        serviceCategory.ifPresent(s -> record.put("SERVICECATEGORY", s));

//        SERVICE_LIMIT_FLAG
        Optional<String> serviceLimitFlag = tx.getServiceLimitFlag();
        serviceLimitFlag.ifPresent(s -> record.put("SERVICELIMIT_FLAG", s));

//        DOCUMENT_STATUS
        Optional<String> documentStatus = tx.getDocumentStatus();
        documentStatus.ifPresent(s -> record.put("DOCUMENTSTATUS", s));

//        DOCUMENT_STATUS_TIME
        Optional<String> documentStatusTime = tx.getDocumentStatusTime();
        documentStatusTime.ifPresent(s -> record.put("DOCUMENTSTATUS_TIME", s));

//        IS_REG_CUST
        Optional<String> isRegCust = tx.getIsRegCust();
        isRegCust.ifPresent(s -> record.put("ISREG_CUST", s));

//        EVENT_DATE
        Optional<String> genFullDate = tx.getEventDate();
        genFullDate.ifPresent(s -> record.put("EVENT_DATE", s));

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        LocalDateTime ldt = LocalDateTime.now();
        record.put("POPULATION_DATE",dtf.format(ldt));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
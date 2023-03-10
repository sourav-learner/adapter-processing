package com.gamma.skybase.build.server.etl.decoder.crm_subs_all;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Optional;

public class CrmSubsAllRecordEnrichment implements IEnrichment {

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CrmSubsAllEnrichmentUtil tx = CrmSubsAllEnrichmentUtil.of(record);

//        SUBS_TYPE
        Optional<String> subType = tx.getSubType();
        subType.ifPresent(s -> record.put("SUBS_TYPE", s));

//        SERVED_MSISDN
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> record.put("SERVED_MSISDN",s));

//        SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey(record.get("SERVED_MSISDN").toString());
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY" ,s));

//        SUBLEVEL
        Optional<String> subLevel = tx.getSubLevel();
        subLevel.ifPresent(s -> record.put("SUBLEVEL",s));

//        DUNFLAG
        Optional<String> dunFlag = tx.getDunFlag();
        dunFlag.ifPresent(s -> record.put("DUNFLAG",s));

//        CREATEDATE
        Optional<String> createDate = tx.getCreateDate();
        createDate.ifPresent(s -> record.put("CREATEDATE", s));

//        AGREEMENTDATE
        Optional<String> agreementDate = tx.getAgreementDate();
        agreementDate.ifPresent(s -> record.put("AGREEMENTDATE", s));

//        FIRSTACTIVATION_DATE
        Optional<String> firstActivationDate = tx.getFirstActivationDate();
        firstActivationDate.ifPresent(s -> record.put("FIRSTACTIVATION_DATE", s));

//        EFFDATE
        Optional<String> effDate = tx.getEffDate();
        effDate.ifPresent(s -> record.put("EFFDATE", s));

//        EXPY_DATE
        Optional<String> expDate = tx.getExpDate();
        expDate.ifPresent(s -> record.put("EXPY_DATE", s));

//        MODDATE
        Optional<String> modDate = tx.getModDate();
        modDate.ifPresent(s -> record.put("MODDATE", s));

//        ACTIVEDATE
        Optional<String> activeDate = tx.getActiveDate();
        activeDate.ifPresent(s -> record.put("ACTIVEDATE", s));

//        LATESTACTIVE_DATE
        Optional<String> latestActiveDate = tx.getLatestActiveDate();
        latestActiveDate.ifPresent(s -> record.put("LATESTACTIVE_DATE", s));

//        EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

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
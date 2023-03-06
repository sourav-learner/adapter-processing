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

//        SUB_TYPE
        Optional<String> subType = tx.getSubType();
        subType.ifPresent(s -> record.put("SUBS_TYPE", s));

//        SERVED_MSISDN
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> record.put("SERVED_MSISDN",s));

//        SRV_TYPE_KEY
        Optional<String> srvTypeKey = tx.getSrvTypeKey(record.get("SERVED_MSISDN").toString());
        srvTypeKey.ifPresent(s -> record.put("SRV_TYPE_KEY" ,s));

//        SUB_LEVEL
        Optional<String> subLevel = tx.getSubLevel();
        subLevel.ifPresent(s -> record.put("SUB_LEVEL",s));

//        DUN_FLAG
        Optional<String> dunFlag = tx.getDunFlag();
        dunFlag.ifPresent(s -> record.put("DUN_FLAG",s));

//        CREATE_DATE
        Optional<String> createDate = tx.getCreateDate();
        createDate.ifPresent(s -> record.put("CREATE_DATE", s));

//        AGREEMENT_DATE
        Optional<String> agreementDate = tx.getAgreementDate();
        agreementDate.ifPresent(s -> record.put("AGREEMENT_DATE", s));

//        FIRST_ACTIVATION_DATE
        Optional<String> firstActivationDate = tx.getFirstActivationDate();
        firstActivationDate.ifPresent(s -> record.put("FIRST_ACTIVATION_DATE", s));

//        EFF_DATE
        Optional<String> effDate = tx.getEffDate();
        effDate.ifPresent(s -> record.put("EFF_DATE", s));

//        EXP_DATE
        Optional<String> expDate = tx.getExpDate();
        expDate.ifPresent(s -> record.put("EXP_DATE", s));

//        MOD_DATE
        Optional<String> modDate = tx.getModDate();
        modDate.ifPresent(s -> record.put("MOD_DATE", s));

//        ACTIVE_DATE
        Optional<String> activeDate = tx.getActiveDate();
        activeDate.ifPresent(s -> record.put("ACTIVE_DATE", s));

//        LATEST_ACTIVE_DATE
        Optional<String> latestActiveDate = tx.getLatestActiveDate();
        latestActiveDate.ifPresent(s -> record.put("LATEST_ACTIVE_DATE", s));

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
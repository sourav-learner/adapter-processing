package com.gamma.skybase.build.server.etl.decoder.uvc_supply;


import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class UvcsupplyRecordEnrichment implements IEnrichment {

    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        UvcsupplyEnrichmentUtil tx = UvcsupplyEnrichmentUtil.of(record);

        //FACEVALUE
        Optional<String> faceValue = tx.getFaceValue();
        faceValue.ifPresent(s -> record.put("DENOMINATION", s));

        //CARD_START_DATE, XDR_DATE
        Optional<String> createDate = tx.getCardStateDate();
        createDate.ifPresent(s ->{
            record.put("CARD_START_DATE", s);
            record.put("XDR_DATE", s);
        });

        //CARD_STOP_DATE
        Optional<String> endTime = tx.getCardStopDate();
        endTime.ifPresent(s -> record.put("CARD_STOP_DATE", s));

        //TRADETIME
        Optional<String> tradeTime = tx.getTradeTime();
        tradeTime.ifPresent(s -> record.put("RECHARGE_TIMESTAMP", s));

        //SERVED_MSISDN
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> record.put("SERVED_MSISDN",s));

        //OPRTYPE_DESC
        Optional<String> oprTypeDesc = tx.getOprTypeDesc();
        oprTypeDesc.ifPresent(s -> record.put("OPRTYPE_DESC", s));

        //USE_STATE_DESC
        Optional<String> useStateDesc = tx.getUseStateDesc();
        useStateDesc.ifPresent(s -> record.put("USE_STATE_DESC", s));

        //HOT_CARD_FLAG_DESC
        Optional<String> hotCardFlagDesc = tx.getHotCardFlagDesc();
        hotCardFlagDesc.ifPresent(s -> record.put("HOT_CARD_FLAG_DESC", s));

        //RECHARGE_TYPE
        Optional<String> rechargeType = tx.getRechargeType();
        rechargeType.ifPresent(s -> record.put("RECHARGE_TYPE", s));


        //FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

        //EVENT_DATE
        Optional<String> eventDate = tx.getEventDate();
        eventDate.ifPresent(s -> record.put("EVENT_DATE", s));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
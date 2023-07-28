package com.gamma.skybase.build.server.etl.decoder.uvc_manlog;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class UvcManlogRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        UvcManlogEnrichmentUtil tx = UvcManlogEnrichmentUtil.of(record);

//        FACEVALUE
        Optional<String> faceValue = tx.getFaceValue();
        faceValue.ifPresent(s -> record.put("DENOMINATION", s));

//        CARDSTARTDATE , XDR_DATE
        Optional<String> createDate = tx.getCardStateDate();
        createDate.ifPresent(s ->{
            record.put("CARD_START_DATE", s);
            record.put("XDR_DATE", s);
        });

//        CARDSTOPDATE
        Optional<String> endTime = tx.getCardStopDate();
        endTime.ifPresent(s -> record.put("CARD_STOP_DATE", s));

//        SERVED_MSISDN
        Optional<String> servedMSISDN = tx.getServedMSISDN();
        servedMSISDN.ifPresent(s -> record.put("SERVED_MSISDN",s));

//        RECHARGE_TIMESTAMP
        Optional<String> tradeTime = tx.getTradeTime();
        tradeTime.ifPresent(s -> record.put("RECHARGE_TIMESTAMP", s));

//        RECHARGE_MODE
        Optional<String> extractFirstDigit = tx.getExtractFirstDigit();
        extractFirstDigit.ifPresent(s -> record.put("RECHARGE_MODE", s));

//        TRADETYPE_RECHARGE
        Optional<String> extractSecondDigit = tx.getExtractSecondDigit();
        extractSecondDigit.ifPresent(s -> record.put("TRADETYPE_RECHARGE", s));

//        TRADETYPE
        Optional<String> extractSixthDigit = tx.getExtractSixthDigit();
        extractSixthDigit.ifPresent(s -> record.put("RECHARGE_METHOD", s));

//        RECHARGE_ROLLOUT
        Optional<String> extractSeventhDigit = tx.getExtractSeventhDigit();
        extractSeventhDigit.ifPresent(s -> record.put("RECHARGE_ROLLOUT", s));

//        RECHARGE_PAYMENT_TYPE
        Optional<String> extractEighthDigit = tx.getExtractEighthDigit();
        extractEighthDigit.ifPresent(s -> record.put("RECHARGE_PAYMENT_TYPE", s));

//        ERRORTYPE_DESC
        Optional<String> errorStatus = tx.getErrorTypeDesc();
        errorStatus.ifPresent(s -> record.put("ERRORTYPE_DESC", s));

//        RECHARGE_TYPE
        Optional<String> dealerStatus = tx.getRechargeType();
        dealerStatus.ifPresent(s -> record.put("RECHARGE_TYPE", s));

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        POPULATION_DATE
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

//        EVENT_DATE
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

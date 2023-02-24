package com.gamma.skybase.build.server.etl.decoder.hlr;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class HlrRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        HlrEnrichmentUtil tx = HlrEnrichmentUtil.of(record);

//        NETWORK_ACCESS_MODE
        Optional<String> networkAccessMode = tx.getNetworkAccessMode();
        networkAccessMode.ifPresent(s -> record.put("NETWORK_ACCESS_MODE", s));

//        ODB_INCOMING_CALL
        Optional<String> odbIncomingCall = tx.getOdbIncomingCall();
        odbIncomingCall.ifPresent(s -> record.put("ODB_INCOMING_CALL", s));

//        ODB_OUTGOING_CALL
        Optional<String> odbOutgoingCall = tx.getOdbOutgoingCall();
        odbOutgoingCall.ifPresent(s -> record.put("ODB_OUTGOING_CALL", s));

//        ODBPLMN1
        Optional<String> obdPlamn11 = tx.getObdPlmn1();
        obdPlamn11.ifPresent(s -> record.put("ODBPLMN1",s));

//        ODBPLMN2
        Optional<String> obdPlamn21 = tx.getObdPlmn2();
        obdPlamn21.ifPresent(s -> record.put("ODBPLMN2",s));

//        ODBPLMN3
        Optional<String> obdPlamn31 = tx.getObdPlmn3();
        obdPlamn31.ifPresent(s -> record.put("ODBPLMN3",s));

//        ODBPLMN4
        Optional<String> obdPlamn41 = tx.getObdPlmn4();
        obdPlamn41.ifPresent(s -> record.put("ODBPLMN4",s));

//        ODB_ROAM
        Optional<String> odbRoam1 = tx.getOdbRoam();
        odbRoam1.ifPresent(s -> record.put("ODB_ROAM",s));

//        ODBDECT_CALLTRANSFER
        Optional<String> odbdectCalltransfer = tx.getOdbdectCalltransfer();
        odbdectCalltransfer.ifPresent(s -> record.put("ODBDECT_CALLTRANSFER",s));

//        ODBMECT_CALLTRANSFER
        Optional<String> odbmectCallTransfer = tx.getOdbmectCallTransfer();
        odbmectCallTransfer.ifPresent(s -> record.put("ODBMECT_CALLTRANSFER",s));

//        ODB_INFO_SERVICE
        Optional<String> odbInfoService = tx.getOdbInfoService();
        odbInfoService.ifPresent(s -> record.put("ODB_INFO_SERVICE",s));

//        ODB_PACKET_ORIENTED_SERVICE
        Optional<String> odbPacketOrientedService = tx.getOdbPacketOrientedService();
        odbPacketOrientedService.ifPresent(s -> record.put("ODB_PACKET_ORIENTED_SERVICE",s));

//        ODB_PACKET_ORIENTED_SERVICE_TYPE
        Optional<String> odbPacketOrientedServiceType = tx.getOdbPacketOrientedServiceType();
        odbPacketOrientedServiceType.ifPresent(s -> record.put("ODB_PACKET_ORIENTED_SERVICE_TYPE",s));

//        ODB_REG_CALL_FWD
        Optional<String> odbRegCallFwd = tx.getOdbRegCallFwd();
        odbRegCallFwd.ifPresent(s -> record.put("ODB_REG_CALL_FWD",s));

//        ODB_PREMIUM
        Optional<String> odbPremium = tx.getOdbPremium();
        odbPremium.ifPresent(s -> record.put("ODB_PREMIUM",s));

//        ODB_SUPP_SERV
        Optional<String> odbSuppServ = tx.getOdbSuppServ();
        odbSuppServ.ifPresent(s -> record.put("ODB_SUPP_SERV",s));

//        UTRANNOTALLOWED_3G
        Optional<String> utrannotallowed_3g = tx.getUtrannotallowed_3G();
        utrannotallowed_3g.ifPresent(s -> record.put("UTRANNOTALLOWED_3G",s));

//        GERANNOTALLOWED_2G
        Optional<String> gerannotallowed_2g = tx.getGerannotallowed_2G();
        gerannotallowed_2g.ifPresent(s -> record.put("GERANNOTALLOWED_2G",s));

//        CARD_TYPE
        Optional<String> cardType = tx.getCardType();
        cardType.ifPresent(s -> record.put("CARD_TYPE",s));

//        OCS
        Optional<String> ocs = tx.getOcs();
        ocs.ifPresent(s -> record.put("OCS",s));

//        TCS
        Optional<String> tcs = tx.getTcs();
        tcs.ifPresent(s -> record.put("TCS",s));

//        UCS
        Optional<String> ucs = tx.getUcs();
        ucs.ifPresent(s -> record.put("UCS",s));

//        SMSC_CAMEL
        Optional<String> smscCamel = tx.getSmscCamel();
        smscCamel.ifPresent(s -> record.put("SMSC_CAMEL",s));

//        SMSMT_CAMEL
        Optional<String> smsmtCamel = tx.getSmsmtCamel();
        smsmtCamel.ifPresent(s -> record.put("SMSMT_CAMEL",s));

//        GPRS_CAMEL
        Optional<String> gprsCamel = tx.getGprsCamel();
        gprsCamel.ifPresent(s -> record.put("GPRS_CAMEL",s));

//        SUPL_SERVICE_CAMEL
        Optional<String> suplServiceCamel = tx.getsuplServiceCamel();
        suplServiceCamel.ifPresent(s -> record.put("SUPL_SERVICE_CAMEL",s));

//        VLRINHPLMN
        Optional<String> vlrinhplmn = tx.getVlrinhplmn();
        vlrinhplmn.ifPresent(s -> record.put("VLRINHPLMN",s));

//        VLRINTERNATIONAL
        Optional<String> vlrinternational = tx.getVlrinternational();
        vlrinternational.ifPresent(s -> record.put("VLRINTERNATIONAL",s));

//        SGSNINHPLMN
        Optional<String> sgsninhplmn = tx.getSgsninhplmn();
        sgsninhplmn.ifPresent(s -> record.put("SGSNINHPLMN",s));

//        SGSNINTERNATIONAL
        Optional<String> sgsninternational = tx.getSgsninternational();
        sgsninternational.ifPresent(s -> record.put("SGSNINTERNATIONAL",s));

//        GLOB_CAHRGING_CHARACTER
        Optional<String> globCahrgingCharacter = tx.getGlobCahrgingCharacter();
        globCahrgingCharacter.ifPresent(s -> record.put("GLOB_CAHRGING_CHARACTER",s));

//        OPT_GPRSTPL_ID
        Optional<String> optGprstplId = tx.getOptGprstplId();
        optGprstplId.ifPresent(s -> record.put("OPT_GPRSTPL_ID",s));

//        OKSC
        Optional<String> oksc = tx.getOksc();
        oksc.ifPresent(s -> record.put("OKSC",s));

//        GERAN
        Optional<String> geran = tx.getGeran();
        geran.ifPresent(s -> record.put("GERAN",s));

//       I_HSPA_E
        Optional<String> iHspaE = tx.getIHspaE();
        iHspaE.ifPresent(s -> record.put("I_HSPA_E",s));

//        EUTRAN
        Optional<String> eutran = tx.getEutran();
        eutran.ifPresent(s -> record.put("EUTRAN",s));

//        THREE_GPP
        Optional<String> threeGpp = tx.getThreeGpp();
        threeGpp.ifPresent(s -> record.put("THREE_GPP",s));

//        USER_CATEGORY_M2M
        Optional<String> userCategoryM2m = tx.getUserCategoryM2m();
        userCategoryM2m.ifPresent(s -> record.put("USER_CATEGORY_M2M",s));

//        SMS_OFATPL_ID
        Optional<String> smsOfatpl_Id = tx.getSmsOfatpl_Id();
        smsOfatpl_Id.ifPresent(s -> record.put("SMS_OFATPL_ID",s));

//        SMS_FTN
        Optional<String> smsFtn = tx.getSmsFtn();
        smsFtn.ifPresent(s -> record.put("SMS_FTN",s));

//        FILE_NAME
        record.put("FILE_NAME", record.get("fileName"));

//        XDR_DATE
        Optional<String> eventDate = tx.getXdrDate();
        eventDate.ifPresent(s -> record.put("XDR_DATE", s));

//        POPULATION_DATE
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));

//        EVENT_DATE
        Optional<String> genFullDate = tx.getEventDate();
       genFullDate.ifPresent(s -> record.put("EVENT_DATE", s));

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
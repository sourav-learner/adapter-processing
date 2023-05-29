package com.gamma.skybase.build.server.etl.decoder.cbs_adj;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;

import java.text.SimpleDateFormat;
import java.util.*;

public class CbsAdjRecordEnrichment implements IEnrichment {
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

    public MEnrichmentResponse transform(MEnrichmentReq request) {
        MEnrichmentResponse response = new MEnrichmentResponse();
        LinkedHashMap<String, Object> record = request.getRequest();

        CbsAdjEnrichmentUtil tx = CbsAdjEnrichmentUtil.of(record);

//        SERVED_MSISDN
        String priIdentity = tx.getValue("PRI_IDENTITY");
        if (priIdentity != null) {
            String servedMsisdn;
            if (priIdentity.length() < 12) {
                servedMsisdn = "966" + priIdentity;
            } else {
                servedMsisdn = priIdentity;
            }
            record.put("SERVED_MSISDN", servedMsisdn);
        }

//        RESULT_CODE
        Optional<String> resultCode = tx.getResultCode();
        resultCode.ifPresent(s -> record.put("RESULTCODE", s));

//        ADJUST_AMT
        Optional<String> adjustAmt = tx.getAdjustAmt();
        adjustAmt.ifPresent(s -> record.put("ADJUST_AMT", s));

//        STATUS
        Optional<String> status = tx.getStatus();
        status.ifPresent(s -> record.put("ADJUSTMENT_STATUS", s));

//        ENTRY_DATE
        Optional<String> starTime = tx.getStartTime("ENTRY_DATE");
        starTime.ifPresent(s -> {
            record.put("ENTRYDATE", s);
            record.put("XDR_DATE", s);
        });

//        BC1_OPER_TYPE
        Optional<String> BC1OperType = tx.getBC1OperType();
        BC1OperType.ifPresent(s -> {
            record.put("BC1_OPER_TYPE", s);
            record.put("BC2_OPER_TYPE", s);
            record.put("BC3_OPER_TYPE", s);
            record.put("BC4_OPER_TYPE", s);
            record.put("BC5_OPER_TYPE", s);
            record.put("BC6_OPER_TYPE", s);
            record.put("BC7_OPER_TYPE", s);
            record.put("BC8_OPER_TYPE", s);
            record.put("BC9_OPER_TYPE", s);
            record.put("BC10_OPER_TYPE", s);
        });

//        FC1_Fu_Own_Type
        Optional<String> FC1FuOwnType = tx.getFC1FUOwnType();
        FC1FuOwnType.ifPresent(s -> {
            record.put("FC1_FU_OWN_TYPE", s);
            record.put("FC2_FU_OWN_TYPE", s);
            record.put("FC3_FU_OWN_TYPE", s);
            record.put("FC4_FU_OWN_TYPE", s);
            record.put("FC5_FU_OWN_TYPE", s);
            record.put("FC6_FU_OWN_TYPE", s);
            record.put("FC7_FU_OWN_TYPE", s);
            record.put("FC8_FU_OWN_TYPE", s);
            record.put("FC9_FU_OWN_TYPE", s);
            record.put("FC10_FU_OWN_TYPE", s);
        });

//        FC1_OPER_TYPE
        Optional<String> FC1OperType = tx.getFC1OperType();
        FC1OperType.ifPresent(s -> {
            record.put("FC1_OPER_TYPE", s);
            record.put("FC2_OPER_TYPE", s);
            record.put("FC3_OPER_TYPE",s);
            record.put("FC4_OPER_TYPE",s);
            record.put("FC5_OPER_TYPE",s);
            record.put("FC6_OPER_TYPE",s);
            record.put("FC7_OPER_TYPE",s);
            record.put("FC8_OPER_TYPE",s);
            record.put("FC9_OPER_TYPE",s);
            record.put("FC10_OPER_TYPE",s);
        });

//        BD1_OPER_TYPE
        Optional<String> BD1OperType = tx.getBD1OperType();
        BD1OperType.ifPresent(s -> {
            record.put("BD1_OPER_TYPE", s);
            record.put("BD2_OPER_TYPE", s);
            record.put("BD3_OPER_TYPE",s);
            record.put("BD4_OPER_TYPE",s);
            record.put("BD5_OPER_TYPE",s);
            record.put("BD6_OPER_TYPE",s);
            record.put("BD7_OPER_TYPE",s);
            record.put("BD8_OPER_TYPE",s);
            record.put("BD9_OPER_TYPE",s);
            record.put("BD10_OPER_TYPE",s);
        });

//        FR1_FU_OWN_TYPE
        Optional<String> FR1FuOwnType = tx.getFR1FUOwnType();
        FR1FuOwnType.ifPresent(s -> {
            record.put("FR1_FU_OWN_TYPE", s);
            record.put("FR2_FU_OWN_TYPE", s);
            record.put("FR3_FU_OWN_TYPE", s);
            record.put("FR4_FU_OWN_TYPE", s);
            record.put("FR5_FU_OWN_TYPE", s);
            record.put("FR6_FU_OWN_TYPE", s);
            record.put("FR7_FU_OWN_TYPE", s);
            record.put("FR8_FU_OWN_TYPE", s);
            record.put("FR9_FU_OWN_TYPE", s);
            record.put("FR10_FU_OWN_TYPE", s);
        });

//        FR1_OPER_TYPE
        Optional<String> FR1OperType = tx.getFR1OperType();
        FR1OperType.ifPresent(s -> {
            record.put("FR1_OPERTYPE", s);
            record.put("FR2_OPERTYPE", s);
            record.put("FR3_OPERTYPE", s);
            record.put("FR4_OPERTYPE", s);
            record.put("FR5_OPERTYPE", s);
            record.put("FR6_OPERTYPE", s);
            record.put("FR7_OPERTYPE", s);
            record.put("FR8_OPERTYPE", s);
            record.put("FR9_OPERTYPE", s);
            record.put("FR10_OPERTYPE", s);
        });

//        OLD_STATUS
        Optional<String> oldStatus = tx.getOldStatus();
        oldStatus.ifPresent(s -> {
            record.put("OLD_STATUS", s);
        });

//        CURRENT_STATUS
        Optional<String> currentStatus = tx.getCurrentStatus();
        currentStatus.ifPresent(s -> {
            record.put("CURRENT_STATUS", s);
        });

//      SERVED_TYPE
        Optional<String> payType = tx.getServedType();
        payType.ifPresent(s -> record.put("SERVED_TYPE", s));

        // FILE_NAME , POPULATION_DATE , EVENT_DATE
        record.put("FILE_NAME", record.get("fileName"));
        record.put("POPULATION_DATE", sdfT.get().format(new Date()));
        record.put("EVENT_DATE", tx.genFullDate);

        response.setResponseCode(true);
        response.setResponse(record);
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> record) {
        return record;
    }
}
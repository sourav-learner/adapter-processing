package com.gamma.skybase.build.server.etl.tx.rbt;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.exceptions.AppUnexpectedException;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

public class RBTRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(RBTRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final String opcoName = AppConfig.instance().getProperty("app.datasource.opconame");
    private final String isoCode = AppConfig.instance().getProperty("app.datasource.isocode");
    private final String operatorCode = AppConfig.instance().getProperty("app.datasource.opcode");
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final DecimalFormat decimalFormat = new DecimalFormat("#.000000");

    private final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else response.setResponseCode(false);
        return response;
    }

    @Override
    @SuppressWarnings("Duplicates")
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);

        Object t1 = data.get("TIMESTAMP1");
        if (t1 != null) {
            String fileDateTxt = String.valueOf(t1);
            Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
            record.put("TIMESTAMP1", targetDateFormat.get().format(fileDate));
            record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
        } else {
            throw new AppUnexpectedException("TIMESTAMP1 field is empty in CDR. Row - {}" + new Gson().toJson(data));
        }

        Object t2 = data.get("NEXTBILLINGDATE");
        if (t2 != null) {
            String fileDateTxt = String.valueOf(t2);
            Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
            record.put("NEXTBILLINGDATE", targetDateFormat.get().format(fileDate));
        }

        String aprty = String.valueOf(data.get("APARTY"));
        String msisdn = aprty;
        if (StringUtils.isNotEmpty(aprty) && !aprty.startsWith(countryCode)) {
            msisdn = countryCode + aprty;
        }
        record.put("SERVED_MSISDN", msisdn);

//        String subscribertype = String.valueOf(data.get("SUBSCRIBERTYPE"));
//        String srvType = "2";
//        if (StringUtils.isNotEmpty(aprty) && "postpaid".equalsIgnoreCase(subscribertype)) {
//            srvType = "1";
//        }
//        record.put("SRV_TYPE_KEY", srvType);

        /* CRM Lookup Reference */
        String srvType = getSrvTypeKeyBySubscriber(msisdn);
        record.put("SRV_TYPE_KEY", srvType);

        String cc = String.valueOf(data.get("CALLCHARGE"));
        double callCharge = 0;
        try {
            callCharge = Double.parseDouble(cc);
        } catch (Exception ignored) {
        }
        record.put("CALLCHARGE", callCharge);

        String dcc = String.valueOf(data.get("DEFAULTCALLCHARGE"));
        double defCallCharge = 0;
        double computedCallCharge = defCallCharge;
        try {
            defCallCharge = Double.parseDouble(dcc);
            if (srvType.equalsIgnoreCase("1")) {
                computedCallCharge = defCallCharge;
            } else {
                String rate = transformationLib.getDimLookup("TAX_RATE", "5");
                if (rate != null) {
                    try {
                        computedCallCharge = Double.parseDouble(dcc) * Double.parseDouble(rate);
                    } catch (Exception ignored) {}
                }
            }
        } catch (Exception ignored) {
        }
        record.put("DEFAULTCALLCHARGE", defCallCharge);
        record.put("COMPUTED_CHARGE", computedCallCharge);

        Object t3 = data.get("SUBSCRIPTIONDATE");
        if (t3 != null) {
            String fileDateTxt = String.valueOf(t3);
            Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
            record.put("SUBSCRIPTIONDATE", targetDateFormat.get().format(fileDate));
        }


        record.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));
        return record;
    }

    private String getSrvTypeKeyBySubscriber(String msisdn){
        String srvTypeKey = "2";
        if (StringUtils.isNotEmpty(msisdn)){
            String postpaid = transformationLib.getDimLookupCRMSubscriber(msisdn);
            srvTypeKey = postpaid == null ? "2" : "1";
        }
        return srvTypeKey;
    }
}

package com.gamma.skybase.build.server.etl.tx.dsp;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;

public class DSPRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(DSPRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private static final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            String day = String.valueOf(record.get("Day"));
            String time = String.valueOf(record.get("Time"));
            String fileDateTxt = day + " " + time;
            if (!fileDateTxt.trim().isEmpty()) {
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(fileDate);
                record.put("XDR_DATE", recordTimeTxt);
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));

            /* handling numeric fields */
            String col1 = "Amount";
            try {
                record.put(col1, Double.parseDouble((String) record.get(col1)));
            } catch (Exception e) {
                record.put(col1, 0D);
            }
            String col2 = "Final Charge";
            try {
                record.put(col2, Double.parseDouble((String) record.get(col2)));
            } catch (Exception e) {
                record.put(col2, 0D);
            }


            /* MSISDN enrichment */
            String msisdn = null;
            if (StringUtils.isNotEmpty(String.valueOf(record.get("_msisdnKeyField")))){
                String mk = String.valueOf(record.get("_msisdnKeyField"));
                msisdn = String.valueOf(record.get(mk));
                if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                    msisdn = countryCode + msisdn;
                    record.put(mk, msisdn);
                }
            }
            record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        }catch (Exception ignored){}
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

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if(data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        }
        else response.setResponseCode(false);
        return response;
    }
}

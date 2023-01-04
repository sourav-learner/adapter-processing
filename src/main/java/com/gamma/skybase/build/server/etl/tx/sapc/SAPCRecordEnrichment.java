package com.gamma.skybase.build.server.etl.tx.sapc;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SAPCRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(SAPCRecordEnrichment.class);
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

//        Map<String, Object> modified = new HashMap<>();
//        for (Map.Entry<String, Object> entry: record.entrySet()){
//            if (String.valueOf(entry.getValue()).contains("\\N")){
//                modified.put(entry.getKey(), "");
//            }
//        }
//        record.putAll(modified);

        //cleaning data in following fields as \N is coming in these columns
        record.put("COL_2", "");
        record.put("COL_3", "");
        record.put("COL_4", "");
        record.put("COL_5", "");
        record.put("COL_6", "");
        record.put("COL_12", "");
        record.put("COL_13", "");
        record.put("COL_14", "");
        record.put("COL_15", "");

        String msisdn = String.valueOf(record.get("SERVED_MSISDN"));
        if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)){
            msisdn = countryCode + msisdn;
        }
        record.put("SERVED_MSISDN", msisdn);


        Object fd = data.get("fileDate");
        if (fd != null) {
            String fileDateTxt = String.valueOf(fd);
            Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
            record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
        } else {
            logger.warn("[SAPC] file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
            record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
        }
        record.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));

        return record;
    }
}

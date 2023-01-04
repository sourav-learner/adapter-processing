package com.gamma.skybase.build.server.etl.tx.evd;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

public class EVDMonthlyRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(EVDMonthlyRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object et = record.get("trans_time");
            if (et != null) {
                String cdrDateTxt = String.valueOf(et);
                Date cdrDate = DateUtility.convertWideString2JavaUtilDate(cdrDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(cdrDate);
                record.put("trans_time", recordTimeTxt);
            }

            /* set xdr_date as month end date to put all month data into single partition */
            Object fd = data.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date pmed = DateUtility.getPreviousMonthEndDate(DateUtility.convertWideString2JavaUtilDate(fileDateTxt));
                record.put("XDR_DATE", targetDateFormat.get().format(pmed));
                if (String.valueOf(data.get("fileName")).startsWith("zainrafm_evd_monthly_20220329100914_0001.csv") ){
                    record.put("XDR_DATE", "20220131 23:59:59");
                }
            } else {
                logger.warn("[EVD-Monthly] file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
                record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));

            /* handling numeric fields */
            List<String> numeric = Arrays.asList("balance_after");
            for (String item : numeric) {
                try {
                    record.put(item, Double.parseDouble((String) record.get(item)));
                } catch (Exception e) {
                    record.put(item, 0D);
                }
            }

        }catch (Exception ignored){}
        return record;
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

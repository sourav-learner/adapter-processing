package com.gamma.skybase.build.server.etl.tx.evd;

import com.gamma.components.commons.DateUtility;
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

public class EVDSaleRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(EVDSaleRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object et = record.get("paymentdate");
            if (et != null) {
                String fileDateTxt = String.valueOf(et);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(fileDate);
                record.put("paymentdate", recordTimeTxt);
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));

            /* handling numeric fields */
            List<String> numeric = Arrays.asList("amount");
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

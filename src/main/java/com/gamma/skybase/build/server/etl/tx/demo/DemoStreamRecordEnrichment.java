package com.gamma.skybase.build.server.etl.tx.demo;

import com.gamma.components.commons.DateUtility;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;

public class DemoStreamRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(DemoStreamRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));

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

        Object fd = data.get("population_date_time");
        if (fd != null) {
            String fileDateTxt = String.valueOf(fd);
            Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
            record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
        } else {
            logger.warn("[DEMO-STREAM] file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
            record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
        }
        record.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));
        return record;
    }
}

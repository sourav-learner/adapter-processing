package com.gamma.skybase.build.server.etl.tx.hur;

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
import java.util.*;

@SuppressWarnings("unchecked")
public class HURRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(HURRecordEnrichment.class);
    private static final String SOURCE_FORMAT = "yyyyMMddHHmmss";
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(SOURCE_FORMAT));
    private static final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private final Date currentTime = new Date();
    private LinkedHashMap<String, Object> headerRecord;

    @Override
    public MEnrichmentResponse transform(MEnrichmentReq request) {
        this.headerRecord = (LinkedHashMap<String, Object>) request.getOptionalParams();
        LinkedHashMap<String, Object> data = transform(request.getRequest());
        MEnrichmentResponse response = new MEnrichmentResponse();
        if (data != null) {
            response.setResponseCode(true);
            response.setResponse(data);
        } else {
            response.setResponseCode(false);
        }
        return response;
    }

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {

//        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object REPORT_ANALYSIS_TIME = data.get("REPORT_ANALYSIS_TIME");
            if (REPORT_ANALYSIS_TIME != null)
                data.put("REPORT_ANALYSIS_TIME", sourceDateFormat.get().parse(REPORT_ANALYSIS_TIME.toString()));

            Object REPORT_CREATION_TIME = data.get("REPORT_CREATION_TIME");
            if (REPORT_CREATION_TIME != null)
                data.put("REPORT_CREATION_TIME", sourceDateFormat.get().parse(REPORT_CREATION_TIME.toString()));


            Object fd = data.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                data.put("XDR_DATE", formatter.format(fileDate.toInstant()));
            } else {
                logger.warn("[HUR] file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
                data.put("XDR_DATE", formatter.format(Instant.now()));
            }
            data.put("POPULATION_DATE", formatter.format(Instant.now()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null; /* skipping further processing if txn_date missing */
        }

        return data;
    }
}

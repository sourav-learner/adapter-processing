package com.gamma.skybase.build.server.etl.tx.evd;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
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

public class EVDRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(EVDRecordEnrichment.class);
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
                String fileDateTxt = String.valueOf(et);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(fileDate);
                record.put("trans_time", recordTimeTxt);
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

            /* MSISDN enrichment */
            String msisdn = String.valueOf(data.get("request_for"));
            if (StringUtils.isEmpty(msisdn) || "null".equalsIgnoreCase(msisdn)) {
                msisdn = "";
            }

            String opType = String.valueOf(data.get("operation_type"));
            if ("MVoucher".equalsIgnoreCase(opType)) {
                if (StringUtils.isNotEmpty(msisdn)) {
                    msisdn = StringUtils.stripStart(msisdn, "0");
                    if (!msisdn.startsWith(countryCode)) {
                        msisdn = countryCode + msisdn;
                    }
                }
            }
            record.put("SERVED_MSISDN", msisdn);

            String srcTransid = String.valueOf(data.get("src_transid"));
            if (StringUtils.isNotEmpty(srcTransid) && "null".equalsIgnoreCase(srcTransid)) {
                srcTransid = "";
            }
            record.put("src_transid", srcTransid);
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

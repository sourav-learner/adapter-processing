package com.gamma.skybase.build.server.etl.tx.psms;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.server.etl.tx.ussd.USSDRecordEnrichment;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

/**
 * Created by abhi on 26/01/22
 */
@SuppressWarnings("Duplicates")
public class JinnyPSMSRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(JinnyPSMSRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSS"));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(data);
        try {
            String recordTimestamp = (String) txRecord.get("record_timestamp");
            String fromNumber = (String) txRecord.get("from_number");
            String fileName = (String) txRecord.remove("fileName");
            if (StringUtils.isNumeric(fromNumber)) {
                if (!fromNumber.startsWith(countryCode)) fromNumber = countryCode + fromNumber;
            }
            txRecord.put("SERVED_MSISDN", fromNumber);
            Date startDateTime = sourceDateFormat.get().parse(recordTimestamp);
            txRecord.put("XDR_DATE", targetDateFormat.get().format(startDateTime));
            txRecord.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));
            txRecord.put("FILE_NAME", fileName);
            try {
                txRecord.put("charge_amount", Double.parseDouble((String) data.get("charge_amount")));
            } catch (Exception e) {
                txRecord.put("charge_amount", 0D);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return txRecord;
    }

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
}

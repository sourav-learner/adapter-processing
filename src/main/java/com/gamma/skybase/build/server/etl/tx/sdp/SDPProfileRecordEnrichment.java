package com.gamma.skybase.build.server.etl.tx.sdp;

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

public class SDPProfileRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(SDPProfileRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String TARGET_FORMAT2 = "yyyyMMdd";
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT2));
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);
        try {
            /* adjusting day_id from file_date. DAY_ID = FILE_DATE - 1 */
            String dayId = null;
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
                Object fd = record.get("fileDate");
                if (fd != null) {
                    String fileDateTxt = String.valueOf(fd);
                    Date fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                    record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
                    dayId = targetDateFormat2.get().format(fileDate);
                } else {
                    logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", record.get("fileName"), Instant.now());
                    record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
                    dayId = targetDateFormat2.get().format(new Date());
                }
                record.put("POPULATION_DATE", formatter.format(Instant.now()));
            }catch (Exception ignored){}
            record.put("DAY_ID", dayId);

            /* MSISDN enrichment */
            if (StringUtils.isNotEmpty(String.valueOf(record.get("_msisdnKeyField")))){
                String mk = String.valueOf(record.get("_msisdnKeyField"));
                String msisdn = String.valueOf(record.get(mk));
                if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                    msisdn = countryCode + msisdn;
                    record.put(mk, msisdn);
                }
            }

            /* srv_type via service class */
            String scid = String.valueOf(record.get("SERVICECLASSID"));
            String srvTypeKey = "2";
            try{
                if (Integer.parseInt(scid) >= 700){
                    srvTypeKey = "1";
                }
            }catch (Exception ignored){}
            record.put("SRV_TYPE_KEY", srvTypeKey);
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

package com.gamma.skybase.build.server.etl.tx.hss;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;

public class HSSRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(HSSRecordEnrichment.class);
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = data.get("fileDate");
            if (fd != null){
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                output.put("XDR_DATE", formatter.format(fileDate.toInstant()));
            }else {
                logger.warn("[HSS] file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
                output.put("XDR_DATE", formatter.format(Instant.now()));
            }
            output.put("POPULATION_DATE", formatter.format(Instant.now()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null; /* skipping further processing if txn_date missing */
        }

        /* MSISDN enrichment */
        String mk = "ESMMSISDN";
        if (StringUtils.isNotEmpty(String.valueOf(output.get(mk)))){
            String msisdn = String.valueOf(output.get(mk));
            if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                msisdn = countryCode + msisdn;
                output.put(mk, msisdn);
            }
        }

        return output;
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

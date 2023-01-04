package com.gamma.skybase.build.server.etl.tx.wicash;

import com.gamma.components.commons.DateUtility;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;

public class WiCashRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(WiCashRecordEnrichment.class);
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        try{
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            String cachkdate = String.valueOf(data.get("cachkdate"));
            Date transDate = DateUtility.convertString2JavaUtilDate(cachkdate, SOURCE_FORMAT);
            output.put("cachkdate", formatter.format(transDate.toInstant()));
            output.put("XDR_DATE", formatter.format(transDate.toInstant()));
            output.put("POPULATION_DATE", formatter.format(Instant.now()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null; /* skipping further processing if txn_date missing */
        }
        try{
            output.put("cachkamt_gl", Double.parseDouble((String) data.get("cachkamt_gl")));
        }catch (Exception e){
            output.put("cachkamt_gl", 0D);
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

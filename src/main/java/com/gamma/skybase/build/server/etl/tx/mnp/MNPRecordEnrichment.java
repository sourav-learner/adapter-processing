package com.gamma.skybase.build.server.etl.tx.mnp;

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
import java.util.Date;
import java.util.LinkedHashMap;

/**
 * Created by abhi on 26/12/21
 */
@SuppressWarnings("Duplicates")
public class MNPRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(MNPRecordEnrichment.class);
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));


    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>();
        try {
            txRecord.put("ORDER_ID", data.get("order_id"));
            txRecord.put("CUSTOMER_NAME", data.get("customer_name"));
            txRecord.put("IS_ROUTING_DB_UPDATED", data.get("is_routing_db_updated"));
            txRecord.put("DONOR_OPER", data.get("donor_oper"));
            txRecord.put("RECIPIENT_OPER", data.get("recipient_oper"));
            txRecord.put("STATUS", data.get("status"));

            Object createdTime = data.get("created_time");
            Object modifiedTime = data.get("modified_time");
            String donorOper = (String) data.get("donor_oper");
            String recipientOper = (String) data.get("recipient_oper");

            String actionType = "na";
            if ("Zain".equalsIgnoreCase(donorOper)) actionType = "port-out";
            if ("Zain".equalsIgnoreCase(recipientOper)) actionType = "port-in";
            txRecord.put("ACTION_TYPE", actionType);


            /* MSISDN enrichment */
            String msisdn = String.valueOf(data.get("msisdn"));
            if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                msisdn = countryCode + msisdn;
            }
            txRecord.put("SERVED_MSISDN", msisdn);

            if (createdTime != null) {
                String createdTimeTxt = (String) createdTime;
                Date createdTimeDt = DateUtility.convertWideString2JavaUtilDate(createdTimeTxt);
                txRecord.put("CREATED_TIME", targetDateFormat.get().format(createdTimeDt));
            }

            if (modifiedTime != null) {
                String modifiedTimeTxt = (String) modifiedTime;
                Date modifiedTimeDt = DateUtility.convertWideString2JavaUtilDate(modifiedTimeTxt);
                txRecord.put("MODIFIED_TIME", targetDateFormat.get().format(modifiedTimeDt));
                txRecord.put("XDR_DATE", targetDateFormat.get().format(modifiedTimeDt));
            }else {
                Object fd = data.get("fileDate");
                if (fd != null) {
                    String fileDateTxt = String.valueOf(fd);
                    Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                    txRecord.put("XDR_DATE", targetDateFormat.get().format(fileDate));
                } else {
                    logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
                    txRecord.put("XDR_DATE", targetDateFormat.get().format(new Date()));
                }
            }
            txRecord.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));
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

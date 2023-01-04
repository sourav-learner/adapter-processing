package com.gamma.skybase.build.server.etl.tx.ussd;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by abhi on 26/01/22
 */
@SuppressWarnings("Duplicates")
public class USSDRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(USSDRecordEnrichment.class);
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy:MM:dd HH:mm:ss.SSSSSSS"));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));


    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> txRecord = new LinkedHashMap<>(data);
        try {
            txRecord.remove("fileDate");
            txRecord.remove("fileNode");
            txRecord.remove("node");
            txRecord.remove("datasource");
            txRecord.remove("_msisdnKeyField");
            String fileName = (String) txRecord.remove("fileName");
            String transactionStartDateTime = (String) txRecord.get("transaction_start_datetime");
            String transactionEndDateTime = (String) txRecord.get("transaction_end_datetime");

            txRecord.put("SERVED_MSISDN", txRecord.get("a_msisdn"));
            Date startDateTime = sourceDateFormat.get().parse(transactionStartDateTime);
            Date endDateTime = sourceDateFormat.get().parse(transactionEndDateTime);

            txRecord.put("EVENT_START_TIME", targetDateFormat.get().format(startDateTime));
            txRecord.put("EVENT_END_TIME", targetDateFormat.get().format(endDateTime));
            txRecord.put("XDR_DATE", targetDateFormat.get().format(startDateTime));
            txRecord.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));
            txRecord.put("FILE_NAME", fileName);
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

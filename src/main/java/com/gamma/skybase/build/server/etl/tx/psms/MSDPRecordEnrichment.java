package com.gamma.skybase.build.server.etl.tx.psms;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

public class MSDPRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(MSDPRecordEnrichment.class);
    private static final String SOURCE_FORMAT = "yyyyMMddHHmmss";
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(SOURCE_FORMAT));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);
        try {
            String [] columnArr1 = new String [] {"timestamp", "begintime", "endtime"};
            for (String col: columnArr1){
                try {
                    Object t = data.get(col);
                    if (t != null) {
                        String fileDateTxt = String.valueOf(t);
                        Date fileDate = sourceDateFormat.get().parse(fileDateTxt);
                        record.put(col, targetDateFormat.get().format(fileDate));
                    } else record.put(col, null);
                } catch (Exception e) {
                    record.put(col, null);
                    logger.error(e.getMessage(), e);
                }
            }
            record.put("XDR_DATE", record.get("timestamp"));
            record.put("POPULATION_DATE", targetDateFormat.get().format(new Date()));

            /* handling numeric fields */
            String [] columnArr2 = new String [] {"fee", "actualdebitfee", "feewithouttax"};
            for (String col: columnArr2){
                try {
                    record.put(col, Long.parseLong((String) record.get(col)));
                } catch (Exception e) {
                    record.put(col, 0D);
                }
            }

            String [] columnArr3 = new String [] {"duration", "volume", "uplinkvolume", "downlinkvolume", "billedvolume", "billedduration"};
            for (String col: columnArr3){
                try {
                    record.put(col, Long.parseLong((String) record.get(col)));
                } catch (Exception e) {
                    record.put(col, 0L);
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

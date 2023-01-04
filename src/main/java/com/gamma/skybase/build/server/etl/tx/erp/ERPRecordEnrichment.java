package com.gamma.skybase.build.server.etl.tx.erp;

import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

public class ERPRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(ERPRecordEnrichment.class);
    private static final String SOURCE_FORMAT = "dd-MMM-yy";
    private static final String TARGET_FORMAT1 = "yyyy-MM-dd";
    private static final String TARGET_FORMAT2 = "yyyyMMdd HH:mm:ss";
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(SOURCE_FORMAT));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat1 = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT1));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT2));

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>(data);
        try {
            String idc = "Insert Date";
            Object t1 = data.get(idc);
            if (t1 != null) {
                String fileDateTxt = String.valueOf(t1);
                Date fileDate = sourceDateFormat.get().parse(fileDateTxt);
                record.put(idc, targetDateFormat1.get().format(fileDate));
            }

            String tdc = "Trans Date";
            Object t2 = data.get(tdc);
            if (t2 != null) {
                String fileDateTxt = String.valueOf(t2);
                Date fileDate = sourceDateFormat.get().parse(fileDateTxt);
                record.put(tdc, targetDateFormat1.get().format(fileDate));
                record.put("XDR_DATE", targetDateFormat2.get().format(fileDate));
            }

            record.put("POPULATION_DATE", targetDateFormat2.get().format(new Date()));

            /* handling numeric fields */
            String col1 = "Finalcharge Wo Tax";
            try {
                record.put(col1, Double.parseDouble((String) record.get(col1)));
            } catch (Exception e) {
                record.put(col1, 0D);
            }
            String col2 = "Charged Duration";
            try {
                record.put(col2, Long.parseLong((String) record.get(col2)));
            } catch (Exception e) {
                record.put(col2, 0L);
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

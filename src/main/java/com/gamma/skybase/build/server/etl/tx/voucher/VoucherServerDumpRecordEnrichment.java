package com.gamma.skybase.build.server.etl.tx.voucher;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.skybase.build.server.etl.utils.DMSSaleRecord;
import com.gamma.skybase.build.server.service.SkybaseCacheServiceHandler;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;

import static com.gamma.components.commons.AppUtility.getValueOrDefault;

/**
 * Created by abhi on 14/05/20
 */
public class VoucherServerDumpRecordEnrichment implements IEnrichment {
    private static final Logger logger = LoggerFactory.getLogger(VoucherServerDumpRecordEnrichment.class);
    private static final String SOURCE_FORMAT = "yyyyMMdd'T'HH:mm:ssZ";
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String COUNTRY_CODE = AppConfig.instance().getProperty("app.datasource.countrycode");
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
    private static final int VS_DIVIDE_FACTOR = 100;

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        String c1 = "Voucher Creation Date";
        output.put(c1, transformDateColumn(getValueOrDefault(data.get(c1))));

        String c2 = "Voucher Expiry Date and Time";
        output.put(c2, transformDateColumn(getValueOrDefault(data.get(c2))));

        String c3 = "Voucher State Change Date and Time";
        output.put(c3, transformDateColumn(getValueOrDefault(data.get(c3))));

        String c4 = "Subscriber Id";
        String subscriberId = getValueOrDefault(data.get(c4));
        if (!subscriberId.isEmpty() && !subscriberId.startsWith(COUNTRY_CODE)) {
            subscriberId = StringUtils.stripStart(subscriberId, "0");
            subscriberId = COUNTRY_CODE + subscriberId;
        }
        output.put(c4, subscriberId);

        String c5 = "Value(MRP)";
        try {
            double d = Double.parseDouble((String) output.get(c5))/VS_DIVIDE_FACTOR;
            output.put("VOUCHER_VALUE", d);
        } catch (Exception e) {
            output.put("VOUCHER_VALUE", 0D);
        }

        Date fileDate = null;
        try {
            Object fd = output.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
//                fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                fileDate = DateUtility.subtractDays(DateUtility.getMonthStartDate(DateUtility.convertWideString2JavaUtilDate(fileDateTxt)), 1);
                output.put("XDR_DATE", formatter.format(fileDate.toInstant()));
            }
        } catch (Exception ignored) {}
        output.put("POPULATION_DATE", formatter.format(Instant.now()));

        /*  Voucher State
            0 - available
            1 - used
            4 - pending
            5 - unavailable
            6 - reserved
        */
//        String serial = String.valueOf(output.get("Serial No"));
//        String state = String.valueOf(output.get("State"));
//        String recordType = String.valueOf(output.get("Record Type(C/H)"));

        /* enrich dealer_name, equip_name, submission_time, year, month, age_days from dms cache */
//        if ("0".equalsIgnoreCase(state) && "C".equalsIgnoreCase(recordType) && !serial.startsWith("5")){
//            DMSSaleRecord dmsSaleRecord = SkybaseCacheServiceHandler.instance().getDMSSalesLookup(Long.parseLong(serial));
//            if (dmsSaleRecord != null){
//                output.put("SUBMISSION_DATE", formatter.format(dmsSaleRecord.getSubmissionDate().toInstant()));
//                output.put("DEALER_NAME",     dmsSaleRecord.getName());
//                output.put("EQUIP_NAME",      dmsSaleRecord.getEquipName());
//                output.put("YEAR_ID",         dmsSaleRecord.getYearId());
//                output.put("MONTH_NAME",      dmsSaleRecord.getMonthName());
//                output.put("AGE_DAYS",        DateUtility.getNumberOfDaysBetween(dmsSaleRecord.getSubmissionDate(), fileDate, true, true));
//            }
//        }
        return output;
    }

    private String transformDateColumn(String inputTextDate) {
        if(!inputTextDate.isEmpty()) {
            try {
                Date date = DateUtility.convertString2JavaUtilDate(inputTextDate, SOURCE_FORMAT);
                return formatter.format(date.toInstant());
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return "";
    }

    @Override
    @SuppressWarnings("Duplicates")
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

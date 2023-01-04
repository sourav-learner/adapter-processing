package com.gamma.skybase.build.server.etl.tx.ascii;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.exceptions.AppUnexpectedException;
import com.gamma.skybase.build.server.service.SkybaseCacheServiceHandler;
import com.gamma.skybase.build.utility.IntlZoneConfigManager;
import com.gamma.skybase.build.utility.MIntlZone;
import com.gamma.skybase.build.utility.TrunkGroupConfigManager;
import com.gamma.skybase.contract.decoders.IEnrichment;
import com.gamma.skybase.contract.decoders.MEnrichmentReq;
import com.gamma.skybase.contract.decoders.MEnrichmentResponse;
import com.gamma.telco.OpcoBusinessTransformation;
import com.gamma.telco.opco.ReferenceDimDialDigit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CommonAsciiRecordEnrichment implements IEnrichment {

    private static final Logger logger = LoggerFactory.getLogger(CommonAsciiRecordEnrichment.class);
    private final String countryCode = AppConfig.instance().getProperty("app.datasource.countrycode");
    private static final String SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String TARGET_FORMAT = "yyyyMMdd HH:mm:ss";
    private static final String TARGET_FORMAT2 = "yyyyMMdd";
    private final ThreadLocal<SimpleDateFormat> targetDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT));
    private final ThreadLocal<SimpleDateFormat> targetDateFormat2 = ThreadLocal.withInitial(() -> new SimpleDateFormat(TARGET_FORMAT2));
    private final ThreadLocal<SimpleDateFormat> sourceDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(SOURCE_FORMAT));
    private static final OpcoBusinessTransformation transformationLib = new OpcoBusinessTransformation();

    @Override
    public LinkedHashMap<String, Object> transform(LinkedHashMap<String, Object> data) {
        LinkedHashMap<String, Object> output = new LinkedHashMap<>(data);
        /* generic enrichment */
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = data.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                output.put("XDR_DATE", targetDateFormat.get().format(fileDate));
            } else {
                logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", data.get("fileName"), Instant.now());
                output.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            output.put("POPULATION_DATE", formatter.format(Instant.now()));
        }catch (Exception ignored){}

        /* MSISDN enrichment */
        if (StringUtils.isNotEmpty(String.valueOf(output.get("_msisdnKeyField")))){
            String mk = String.valueOf(output.get("_msisdnKeyField"));
            String msisdn = String.valueOf(output.get(mk));
            if (StringUtils.isNotEmpty(msisdn) && !msisdn.startsWith(countryCode)) {
                msisdn = countryCode + msisdn;
                output.put(mk, msisdn);
            }
        }


        /* specific enrichment */
        String adapterName = String.valueOf(data.get("datasource"));
        if ("sdp-main".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPMainRecord(output);
            } catch (ParseException e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }else if ("sdp-da".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPDARecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }else if ("sdp-ua".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPUARecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }else if ("sdp-uc".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPUCRecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }else if ("sdp-ut".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPUTRecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }else if ("sdp-faf".equalsIgnoreCase(adapterName)){
            try {
                output = enrichSDPFAFRecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }
        else if ("cbio-subscriber".equalsIgnoreCase(adapterName)){
            try {
                output = enrichCBiOSubscriberRecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }
        else if ("evd-dealer".equalsIgnoreCase(adapterName)){
            try {
                output = enrichCBiOEVDDealerRecord(output);
            } catch (Exception e) {
                throw new AppUnexpectedException(e.getMessage());
            }
        }
        return output;
    }

    private LinkedHashMap<String, Object> enrichSDPFAFRecord(LinkedHashMap<String, Object> record) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = record.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
            } else {
                logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", record.get("fileName"), Instant.now());
                record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));
        }catch (Exception ignored){}

        /* FAF ID enrichment */
        String fafId = String.valueOf(record.get("FAF_ID"));
        if (StringUtils.isEmpty(fafId) || "null".equalsIgnoreCase(fafId)) {
            fafId = "";
        }
        if (StringUtils.isNotEmpty(fafId)) {
            fafId = StringUtils.stripStart(fafId, "0");
            if (!fafId.startsWith(countryCode)) {
                fafId = countryCode + fafId;
                record.put("FAF_ID", fafId);
            }
        }

        String msisdn = String.valueOf(record.get("MSISDN"));
        record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        return record;
    }

    private LinkedHashMap<String, Object> enrichSDPUTRecord(LinkedHashMap<String, Object> record) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = record.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
            } else {
                logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", record.get("fileName"), Instant.now());
                record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));
        }catch (Exception ignored){}

        List<String> numeric = Collections.singletonList("UT_VALUE");
        for (String item : numeric) {
            try {
                record.put(item, Double.parseDouble((String) record.get(item)));
            } catch (Exception e) {
                record.put(item, 0D);
            }
        }

        String msisdn = String.valueOf(record.get("MSISDN"));
        record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        return record;
    }

    private LinkedHashMap<String, Object> enrichSDPUCRecord(LinkedHashMap<String, Object> record) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = record.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
            } else {
                logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", record.get("fileName"), Instant.now());
                record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));
        }catch (Exception ignored){}

        List<String> numeric = Collections.singletonList("UC_VALUE");
        for (String item : numeric) {
            try {
                record.put(item, Double.parseDouble((String) record.get(item)));
            } catch (Exception e) {
                record.put(item, 0D);
            }
        }

        String msisdn = String.valueOf(record.get("MSISDN"));
        record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        return record;
    }

    private LinkedHashMap<String, Object> enrichCBiOSubscriberRecord(LinkedHashMap<String, Object> record) {
        /* srv_type via used_context */
        String srvTypeKey = null;
        String uc = String.valueOf(record.get("used_context"));
        if ("P".equalsIgnoreCase(uc)) srvTypeKey = "1";
        else if ("A".equalsIgnoreCase(uc)) srvTypeKey = "2";

        /* srv_type via service class */
        if (srvTypeKey == null) {
            srvTypeKey = "2";
            String scid = String.valueOf(record.get("service_class_id"));
            try {
                if (Integer.parseInt(scid) >= 700) {
                    srvTypeKey = "1";
                }
            } catch (Exception ignored) {}
        }
        record.put("SRV_TYPE_KEY", srvTypeKey);
        return record;
    }

    private LinkedHashMap<String, Object> enrichCBiOEVDDealerRecord(LinkedHashMap<String, Object> record) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object fd = record.get("fileDate");
            if (fd != null) {
                String fileDateTxt = String.valueOf(fd);
                Date fileDate = DateUtility.subtractDays(DateUtility.convertWideString2JavaUtilDate(fileDateTxt), 1);
                record.put("XDR_DATE", targetDateFormat.get().format(fileDate));
            } else {
                logger.warn("file date missing. file - {}, Assuming processing date as file date i.e - {}", record.get("fileName"), Instant.now());
                record.put("XDR_DATE", targetDateFormat.get().format(new Date()));
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));
        }catch (Exception ignored){}
        return record;
    }

    private LinkedHashMap<String, Object> enrichInterconnectMarginRecord(LinkedHashMap<String, Object> record) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TARGET_FORMAT).withZone(ZoneId.systemDefault());
            Object et = record.get("event_time");
            if (et != null) {
                String fileDateTxt = String.valueOf(et);
                Date fileDate = DateUtility.convertWideString2JavaUtilDate(fileDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(fileDate);
                record.put("XDR_DATE", recordTimeTxt);
                record.put("event_time", recordTimeTxt);
            }

            Object edt = record.get("entry_date_timestamp");
            if (edt != null) {
                String entryDateTxt = String.valueOf(edt);
                Date entryDate = DateUtility.convertWideString2JavaUtilDate(entryDateTxt);
                String recordTimeTxt = targetDateFormat.get().format(entryDate);
                record.put("entry_date_timestamp", recordTimeTxt);
            }
            record.put("POPULATION_DATE", formatter.format(Instant.now()));

            /* handling numeric fields */
            List<String> numeric = Arrays.asList("rated_flat_amount","duration_volume");
            for (String item : numeric) {
                try {
                    record.put(item, Double.parseDouble((String) record.get(item)));
                } catch (Exception e) {
                    record.put(item, 0D);
                }
            }

            /* enrich trunk group via trunk prefix */
            String tgip = String.valueOf(record.get("trunk_group_in_address"));
            String tgop = String.valueOf(record.get("trunk_group_out_address"));

            String iTrunk = tgip;
            String oTrunk = tgop;
            if (TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().containsKey(tgip)){
                iTrunk = TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().get(tgip).getId();
            }else {
                logger.info("Trunk group prefix mapping not found - {}", tgip);
            }
            if (TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().containsKey(tgop)){
                oTrunk = TrunkGroupConfigManager.instance().getTrunkGroupPrefix1Map().get(tgop).getId();
            }else {
                logger.info("Trunk group prefix mapping not found - {}", tgop);
            }
            record.put("IN_TRUNK", iTrunk);
            record.put("OUT_TRUNK", oTrunk);

            /* dial digit analysis */
            // country_iso, zone, dial_code
            String servedMsisdn = String.valueOf(record.get("other_msisdn"));
            ReferenceDimDialDigit sddk = transformationLib.getDialedDigitSettings(servedMsisdn);
            if (sddk != null) {
                record.put("PARTNER_DDK", sddk.getDialDigitKey());
                record.put("PARTNER_COUNTRY", sddk.getIsoCountryCode());
                record.put("PARTNER_OPER", sddk.getProviderDesc());

                String dialCode = sddk.getTargetCountryCode();
                record.put("DIAL_CODE", dialCode);

                /* min_pulse_ind, chargeable_unit_sec, avg_tariff using dial_code*/
                MIntlZone zone = IntlZoneConfigManager.instance().get(dialCode);
                if (zone != null){
                    if ("true".equalsIgnoreCase(zone.getMinPulse())) record.put("MIN_PULSE", "1");
                    else record.put("MIN_PULSE", "0");
                    record.put("ZONE", zone.getZone());

                    double dv = (Double) record.get("duration_volume");
                    double rfa = (Double) record.get("rated_flat_amount");
                    long bp = (long) Math.ceil(dv / 60);
                    record.put("BILLABLE_PULSE", bp);
                    if ("true".equalsIgnoreCase(zone.getMinPulse())){
                        record.put("CHARGEABLE_UNITS", bp);
                        if (bp > 0) record.put("AVG_TARIFF", rfa/bp);
                    }else {
                        record.put("CHARGEABLE_UNITS", dv);
                        if (dv > 0) record.put("AVG_TARIFF", rfa/dv);
                    }
                }else {
                    logger.info("[Interconnect Margin] Urgent action required. Dial Code Mapping not found for - {}", dialCode);
                }
            }else {
                logger.info("[Interconnect Margin] Urgent action required. Dial Digit Mapping not found for - {}", servedMsisdn);
            }
        }catch (Exception ignored){}
        return record;
    }

    private LinkedHashMap<String, Object> enrichSDPMainRecord(LinkedHashMap<String, Object> record) throws ParseException {
        /* ignore the record if accountactivatedflag = 0 & accountactivateddate = null or empty */
//        String accountactivatedflag = String.valueOf(record.get("ACCOUNTACTIVATEDFLAG"));
//        String accountactivateddate = String.valueOf(record.get("ACCOUNTACTIVATEDDATE"));
//        if ("0".equalsIgnoreCase(accountactivatedflag) && StringUtils.isEmpty(accountactivateddate)) {
//            logger.debug("Ignore SDP Main Record. Reason - accountactivatedflag = 0, accountactivateddate = null/empty");
//            return null;
//        }

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

        String msisdn = String.valueOf(record.get("SUBSCRIBERID"));

        /* lookup start balance for today from SDP balance lookup */
        /* Yesterday's end balance should be today's start */
        String prevDayId = DateUtility.convertJavaUtil2ShortStringDate(DateUtility.subtractDays(DateUtility.convertShortString2JavaUtilDate(dayId), 1));
        double sBalance = getSDPAccountStartBalance(msisdn, prevDayId);
        record.put("START_BALANCE", sBalance);
        record.put("DAY_ID", dayId);

        /* srv_type via service class */
        String scid = String.valueOf(record.get("SERVICECLASSID"));
        String srvTypeKey = "2";
        try{
            if (Integer.parseInt(scid) >= 700){
                srvTypeKey = "1";
            }
        }catch (Exception ignored){}
        record.put("SRV_TYPE_KEY", srvTypeKey);
        return record;
    }

    private LinkedHashMap<String, Object> enrichSDPDARecord(LinkedHashMap<String, Object> record) {

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
        List<String> numeric = Collections.singletonList("DA_BALANCE");
        for (String item : numeric) {
            try {
                record.put(item, Double.parseDouble((String) record.get(item)));
            } catch (Exception e) {
                record.put(item, 0D);
            }
        }

        String msisdn = String.valueOf(record.get("MSISDN"));
        record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        return record;
    }

    private LinkedHashMap<String, Object> enrichSDPUARecord(LinkedHashMap<String, Object> record) {

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

        List<String> numeric = Collections.singletonList("UA_VALUE");
        for (String item : numeric) {
            try {
                record.put(item, Double.parseDouble((String) record.get(item)));
            } catch (Exception e) {
                record.put(item, 0D);
            }
        }

        String msisdn = String.valueOf(record.get("MSISDN"));
        record.put("SRV_TYPE_KEY", getSrvTypeKeyBySubscriber(msisdn));
        return record;
    }

    private double getSDPAccountStartBalance(String msisdn, String dayId) {
        if ("true".equalsIgnoreCase(AppConfig.instance().getProperty("app.sdp-balance-lookup.active", "false"))) {
            try {
                return SkybaseCacheServiceHandler.instance().getSDPMainBalanceFromLookup(msisdn, dayId);
            } catch (Exception ignored) {
            }
        }
        return 0d;
    }

    private String getSrvTypeKeyBySubscriber(String msisdn){
        String srvTypeKey = "2";
        if (StringUtils.isNotEmpty(msisdn)){
            String postpaid = transformationLib.getDimLookupCRMSubscriber(msisdn);
            srvTypeKey = postpaid == null ? "2" : "1";
        }
        return srvTypeKey;
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

package com.gamma.skybase.build.server.etl.tx.ccn;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.exceptions.AppUnexpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by abhi on 10/28/2017
 */

public class CCNRecordUtility {

    private static final Logger logger = LoggerFactory.getLogger(CCNRecordUtility.class);

    private String localDateOffset = DateUtility.getDefaultSystemTimeZone();
    private final ThreadLocal<SimpleDateFormat> sdfT = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
    private final ThreadLocal<SimpleDateFormat> yyyyMMddHHmmss = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> yyMMddHHmmss = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyMMddHHmmss"));
    private final ThreadLocal<SimpleDateFormat> yyyyMMdd = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private final ThreadLocal<SimpleDateFormat> yyyyMMddHH = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd HH"));

    private static String DATE_PATTERN_1 = "^\\d{12}([-+]\\d{4})$";
    private static String DATE_PATTERN_2 = "^\\d{14}([-+]\\d{4})$";

    Map<String, Pattern> patterns = new ConcurrentHashMap<String, Pattern>(){{
        put(DATE_PATTERN_1, Pattern.compile(DATE_PATTERN_1));
        put(DATE_PATTERN_2, Pattern.compile(DATE_PATTERN_2));
    }};

    public boolean matches(String pattern, String date){
        if (!patterns.containsKey(pattern)){
            patterns.put(pattern, Pattern.compile(pattern));
        }
        return patterns.get(pattern).matcher(date).matches();
    }

    public String getTimeOffset(String p, String date){
        List<String> matchList = new ArrayList<>();
        Pattern pattern = patterns.get(p);
        Matcher matcher = pattern.matcher(date);
        while (matcher.find()) {
            int count = matcher.groupCount();
            for (int i = 0; i < count + 1; i++) {
                matchList.add(matcher.group(i).trim());
            }
        }
        return matchList.get(1);
    }

    public Map<String, Object> getTimeUsageValues (Object olccTriggerTime, Object ccrTriggerTime,
                                                          Object eventTime, Long timeUnit,
                                                          Long serviceSpecificUnit){
        Map<String, Object> dataset = new HashMap<>();
        Date triggerTime;
        long origDur = 0L;
        if( ccrTriggerTime == null) ccrTriggerTime = olccTriggerTime;
        if( ccrTriggerTime != null){
            try {
                Long dateTime = adjustTriggerDate(ccrTriggerTime);
                triggerTime = new Date(dateTime);
                dataset.put("EVENT_START_TIME", sdfT.get().format(triggerTime));
                dataset.put("GENERATED_FULL_DATE", yyyyMMdd.get().format(triggerTime) + " 00:00:00"); //31
                dataset.put("EVENT_START_TIME_SLOT_KEY", yyyyMMddHH.get().format(triggerTime) + ":00:00");

                Date eventEndTime = yyyyMMddHHmmss.get().parse(eventTime.toString());
                if (serviceSpecificUnit > 0) {
                    origDur = 0;
                }else if (timeUnit == null) {
                    origDur = (eventEndTime.getTime() - triggerTime.getTime()) / 1000;
                } else {
                    origDur = timeUnit;
                }
                dataset.put("EVENT_END_TIME", sdfT.get().format(eventEndTime));
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
                return null;
            }
        }
        dataset.put("ORIGINAL_DUR", origDur);
        return dataset;
    }

    private Long adjustTriggerDate(Object ccrTriggerTime) throws ParseException{
        long dateTime;
        String utcOffsetCode;

        if(localDateOffset == null) localDateOffset = AppConfig.instance().getProperty("app.datasource.timeoffset");
        int localOffsetInMins = ZoneOffset.of(localDateOffset).getTotalSeconds()/ 60;
        if (matches(DATE_PATTERN_1, ccrTriggerTime.toString())){
            dateTime = yyMMddHHmmss.get().parse(ccrTriggerTime.toString()).getTime();
            utcOffsetCode = getTimeOffset(DATE_PATTERN_1, ccrTriggerTime.toString());
        }else if (matches(DATE_PATTERN_2, ccrTriggerTime.toString())){
            dateTime = yyyyMMddHHmmss.get().parse(ccrTriggerTime.toString()).getTime();
            utcOffsetCode = getTimeOffset(DATE_PATTERN_2, ccrTriggerTime.toString());
        }else {
            String msg = "ccrTriggerTime - " + ccrTriggerTime + " unknown format.";
            throw new AppUnexpectedException(msg);
        }
        int cdrOffsetInMins =  ZoneOffset.of(utcOffsetCode).getTotalSeconds()/ 60;
        int offsetDiff = localOffsetInMins - cdrOffsetInMins;
        dateTime += offsetDiff * 60 * 1000;
        return dateTime;
    }
}

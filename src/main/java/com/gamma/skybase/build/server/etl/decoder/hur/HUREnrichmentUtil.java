package com.gamma.skybase.build.server.etl.decoder.hur;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class HUREnrichmentUtil extends LebaraUtil{

    SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");

    SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy H:mm");

    private HUREnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static HUREnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new HUREnrichmentUtil(record);
    }

    public Optional<String> getEventDate(){
        String timeStamp ;
        timeStamp = getValue("TIME_STAMP");
        if (timeStamp != null){
            Date date = null;
            try {
                date = inputFormat.parse(timeStamp);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            String eventDate = outputFormat.format(date);
            return Optional.of(eventDate);
        }
        return Optional.empty();
    }

    public Optional<String> getXdrDate(){
        String timeStamp ;
        timeStamp = getValue("TIME_STAMP");
        if (timeStamp != null){
            DateTimeFormatter inputFormatter1 = DateTimeFormatter.ofPattern("M/dd/yyyy H:mm");
            try {
                LocalDateTime dateTime = LocalDateTime.parse(timeStamp, inputFormatter1);

                DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
                String xdrDate = dateTime.format(outputFormatter);

                return Optional.of(xdrDate);
            } catch (DateTimeParseException e) {
                // handle parse exception here
                e.printStackTrace();
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
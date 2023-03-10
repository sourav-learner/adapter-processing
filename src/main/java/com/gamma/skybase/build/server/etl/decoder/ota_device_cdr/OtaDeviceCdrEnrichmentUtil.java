package com.gamma.skybase.build.server.etl.decoder.ota_device_cdr;

import com.gamma.skybase.build.server.etl.decoder.LebaraUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

public class OtaDeviceCdrEnrichmentUtil extends LebaraUtil{

    SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd");

    SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

    private OtaDeviceCdrEnrichmentUtil(LinkedHashMap<String, Object> record) {
        super(record);
    }

    public static OtaDeviceCdrEnrichmentUtil of(LinkedHashMap<String, Object> record) {
        return new OtaDeviceCdrEnrichmentUtil(record);
    }

    public Optional<String> getEventDate(){
        String timeStamp ;
        timeStamp = getValue("TIMESTAMP");
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
        timeStamp = getValue("TIMESTAMP");
        if (timeStamp != null){
            DateTimeFormatter inputFormatter1 = DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a");
            LocalDateTime dateTime = LocalDateTime.parse(timeStamp, inputFormatter1);

            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
            String xdrDate = dateTime.format(outputFormatter);

            return Optional.of(xdrDate);
        }
        return Optional.empty();
    }
}
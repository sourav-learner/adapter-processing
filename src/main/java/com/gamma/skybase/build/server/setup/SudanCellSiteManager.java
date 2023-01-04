package com.gamma.skybase.build.server.setup;

import com.gamma.components.commons.DateUtility;
import com.gamma.components.commons.FileUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.common.CollectionNames;
import com.gamma.telco.utility.reference.loader.IReferenceLoader;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by abhi on 13/2/16.
 */
@Slf4j
public class SudanCellSiteManager {

    public static List<Object> load() throws IOException {

        List<Object> dataset = new ArrayList<>();
        String fileName = AppConfig.instance().getProperty("app.conf") + File.separator + "business" + File.separator + "DIM_CELL.CSV";
        DelimitedFileDecoder fileDecoder = new DelimitedFileDecoder(fileName);
        List<String> keys = new ArrayList<>();
        while (fileDecoder.hasNext()) {
            Map<String, Object> rec = fileDecoder.next();
            Map<String, Object> data = new HashMap<>();
            Map<String, Object> geoLocation = new HashMap<>();
            List<Double> coordinates = new ArrayList<>();

            coordinates.add(Double.parseDouble(rec.get("longitude").toString()));
            coordinates.add(Double.parseDouble(rec.get("latitude").toString()));
            geoLocation.put("type", "Point");
            geoLocation.put("coordinates", coordinates);

            String cellId = rec.get("cell_id").toString();
            String type = rec.get("type").toString();
            String key = type + "|" + cellId;
            if (!keys.contains(key)){
                keys.add(key);
            }else {
                log.info("Cell duplicate record found for key - {}", key);
                continue;
            }
            data.put("_id", key);
            data.put("cell_id", rec.get("cell_id"));
            data.put("type", rec.get("type"));
            data.put("cell_name", rec.get("cell_name"));
            data.put("cgi_id", rec.get("cgi_id"));
            data.put("site_id", rec.get("site_id"));
            data.put("site_name", rec.get("site_name"));
            data.put("site_code", rec.get("site_code"));
            data.put("site_location", rec.get("site_location"));
            data.put("location_number", rec.get("location_number"));
            data.put("region", rec.get("region"));
            data.put("city", rec.get("city"));
            data.put("state", rec.get("state"));
            data.put("lac", rec.get("lac"));
            data.put("rnc", rec.get("rnc"));
            data.put("rac", rec.get("rac"));
            data.put("rbs_type", rec.get("rbs_type"));
            data.put("bsc", rec.get("bsc"));
            data.put("band", rec.get("band"));
            data.put("tac", rec.get("tac"));
            data.put("cat", rec.get("cat"));
            data.put("community_type", rec.get("community_type"));
            data.put("vendor", rec.get("vendor"));
            data.put("on_air_date", rec.get("on_air_date"));
            data.put("carrier_count", Double.parseDouble(rec.get("carrier_count").toString()));
            data.put("direction", Double.parseDouble(rec.get("direction").toString()));
            data.put("latitude", Double.parseDouble(rec.get("latitude").toString()));
            data.put("longitude", Double.parseDouble(rec.get("longitude").toString()));
            data.put("geo_location", geoLocation);
            data.put("update_time", DateUtility.getTodayLocalDate());

            dataset.add(data);
        }
        return dataset;
    }
}

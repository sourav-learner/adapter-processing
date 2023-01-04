package com.gamma.skybase.build.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamma.components.commons.FileUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.commons.strategy.JSONFileReloader;
import lombok.extern.slf4j.Slf4j;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class IntlZoneConfigManager extends JSONFileReloader implements Serializable {

    private static Map<String, MIntlZone> dialCodes = new ConcurrentHashMap<>();
    private static List<MIntlZone> array = new ArrayList<>();
    private static IntlZoneConfigManager manager;

    public static IntlZoneConfigManager instance() {
        if (manager == null) {
            manager = new IntlZoneConfigManager();
        }
        return manager;
    }

    private IntlZoneConfigManager() {
        super(AppConfig.instance().getProperty("app.international-zone.config-file"));
    }

    @Override
    public void reload(String content) {
        try {
            JSONArray jsonArray = JSONArray.fromObject(content);
            for (Object o : jsonArray) {
                JSONObject jo = (JSONObject) o;
                ObjectMapper mapper = new ObjectMapper();
                MIntlZone iz = mapper.readValue(String.valueOf(jo), MIntlZone.class);
                array.add(iz);
                if (!dialCodes.containsKey(iz.getDialCode())){
                    dialCodes.put(iz.getDialCode(), iz);
                }else {
                    System.out.println("Duplicates detected. Dial Code - " + iz.getDialCode());
                }
            }
            log.info("International zones config loaded.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public synchronized void reload() {
        try {
            reload(FileUtility.getFileContent(new File(Paths.get(AppConfig.instance().getProperty("app.international-zone.config-file")).toUri())));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public List<MIntlZone> getAll() {
        return array;
    }

    public MIntlZone get(String dialCode) {
        return dialCodes.get(dialCode);
    }
}

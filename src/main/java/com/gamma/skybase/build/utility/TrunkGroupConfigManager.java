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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TrunkGroupConfigManager extends JSONFileReloader implements Serializable {

    private static Map<String, MTrunkGroup> trunks = new ConcurrentHashMap<>();
    private static List<MTrunkGroup> array = new ArrayList<>();
    private static Map<String, MTrunkGroup> trunkPrefix1Container = new ConcurrentHashMap<>();
    private static TrunkGroupConfigManager manager;

    public static TrunkGroupConfigManager instance() {
        if (manager == null) {
            manager = new TrunkGroupConfigManager();
        }
        return manager;
    }

    private TrunkGroupConfigManager() {
        super(AppConfig.instance().getProperty("app.trunk-group.config-file"));
    }

    @Override
    public void reload(String content) {
        try {
            JSONArray jsonArray = JSONArray.fromObject(content);
            for (Object o : jsonArray) {
                JSONObject jo = (JSONObject) o;
                ObjectMapper mapper = new ObjectMapper();
                MTrunkGroup tg = mapper.readValue(String.valueOf(jo), MTrunkGroup.class);
                array.add(tg);
                if (!trunks.containsKey(tg.getId())){
                    trunks.put(tg.getId(), tg);
                }else {
                    System.out.println("Duplicates detected. ID - " + tg.getId());
                }
                if (!trunkPrefix1Container.containsKey(tg.getTrunkprefix1())){
                    trunkPrefix1Container.put(tg.getTrunkprefix1(), tg);
                }
            }
            log.info("Trunk group config loaded.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public synchronized void reload() {
        try {
            reload(FileUtility.getFileContent(new File(Paths.get(AppConfig.instance().getProperty("app.trunk-group.config-file")).toUri())));
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public Collection<MTrunkGroup> getTrunkGroups() {
        return trunks.values();
    }

    public Map<String, MTrunkGroup> getTrunkGroupMap() {
        return trunks;
    }

    public Map<String, MTrunkGroup> getTrunkGroupPrefix1Map() {
        return trunkPrefix1Container;
    }

    public List<MTrunkGroup> getAll(){ return array; }
}

package com.gamma.skybase.build;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gamma.components.commons.FileUtility;
import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.exceptions.AppConfigurationException;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class OpFreeNumberManager {
    private static final Logger logger = LoggerFactory.getLogger(OpFreeNumberManager.class);
    private static OpFreeNumberManager service;
    private static MFreeNumbers freeNumbers;

    public static OpFreeNumberManager instance(){
        if (service == null) {
            service = new OpFreeNumberManager();
        }
        return service;
    }

    private OpFreeNumberManager(){
        freeNumbers = new MFreeNumbers();
        load();
    }

    public synchronized void reload(){
        load();
    }

    private void load(){
        String fileName = AppConfig.instance().getProperty("app.free-number.config-file");
            try {
                File file = new File(fileName);
                String jsonStr = FileUtility.getFileContent(file);
                JSONObject jo = JSONObject.fromObject(jsonStr);
                ObjectMapper om = new ObjectMapper();
                freeNumbers = om.readValue(jo.toString(), MFreeNumbers.class);
                logger.info("App free number config file loaded - " + file.getAbsolutePath());
            } catch (Exception e) {
                throw new AppConfigurationException("Failed loading App free number config file : " + fileName);
            }
    }

    public MFreeNumbers getConfiguredFreeNumbers(){
        if (freeNumbers == null){
            reload();
        }
        return freeNumbers;
    }
}

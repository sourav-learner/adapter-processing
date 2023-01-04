package com.gamma.skybase.build;

import com.gamma.components.commons.app.AppConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Properties;

/**
 * Created by abhi on 2/5/2017.
 */

@Slf4j
public class BootSettings {

    private static BootSettings setup;
    private Properties prop;
    private String sep = File.separator;
    private String bootFile = AppConfig.instance().getProperty("app.conf")+sep+"boot.init";

    public synchronized static BootSettings instance(){
        if(setup == null){
            setup = new BootSettings();
        }
        return setup;
    }

    private BootSettings(){
        prop = new Properties();
        try {
            InputStream stream = new FileInputStream(bootFile);
            prop.load(stream);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public boolean isSetupDone(){
        return "true".equalsIgnoreCase(prop.getProperty("SETUP_DONE"));
    }

    public void markSetupDone(){
        try {
            OutputStream output = new FileOutputStream(bootFile);
            prop.setProperty("SETUP_DONE","true");
            prop.store(output, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

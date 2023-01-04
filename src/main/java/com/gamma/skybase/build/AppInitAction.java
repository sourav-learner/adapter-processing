package com.gamma.skybase.build;

import com.gamma.components.cache.core.ServerCache;
import com.gamma.components.commons.IAppInitAction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppInitAction implements IAppInitAction {

    @Override
    public void start() {
        if (!AppSetup.instance().isSetupDone()) {
            log.info("Loading zain skybase setup files");
            try{
                AppSetup.instance().setup();
            }catch (Exception e){
                log.error(e.getMessage(), e);
            }
        }
        ServerCache.instance();
    }

    @Override
    public void stop() {

    }
}

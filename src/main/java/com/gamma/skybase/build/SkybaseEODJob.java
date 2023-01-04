package com.gamma.skybase.build;

import com.gamma.components.cache.core.ServerCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SkybaseEODJob {

    private static final Logger logger = LoggerFactory.getLogger(SkybaseEODJob.class);

    @Scheduled(cron = "* 5 12 ? * *")
    public void execute() {
        try {
            logger.info("Reloading param cache.");
            ServerCache.instance().refreshCache();
            logger.info("Param cache reloaded.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(0);
        }
    }
}

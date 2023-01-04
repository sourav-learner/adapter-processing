package com.gamma.skybase.build.console;

import com.gamma.components.cache.core.ServerCache;
import com.gamma.skybase.build.MFreeNumbers;
import com.gamma.skybase.build.OpFreeNumberManager;
import com.gamma.skybase.build.utility.IntlZoneConfigManager;
import com.gamma.skybase.build.utility.MIntlZone;
import com.gamma.skybase.build.utility.MTrunkGroup;
import com.gamma.skybase.build.utility.TrunkGroupConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping("/api/v1/skybase/")
public class SkybaseController {
    private static final Logger logger = LoggerFactory.getLogger(SkybaseController.class);

    @RequestMapping(value = "free-numbers/all", method = RequestMethod.GET)
    public @ResponseBody
    MFreeNumbers getAllFreeNumbers() {
        return OpFreeNumberManager.instance().getConfiguredFreeNumbers();
    }

    @RequestMapping(value = "free-numbers/toll-free", method = RequestMethod.GET)
    public @ResponseBody
    List<String> getTollFreeNumbers() {
        return OpFreeNumberManager.instance().getConfiguredFreeNumbers().getTollfreeNumbers();
    }

    @RequestMapping(value = "free-numbers/short-numbers", method = RequestMethod.GET)
    public @ResponseBody
    List<String> getShortNumbers() {
        return OpFreeNumberManager.instance().getConfiguredFreeNumbers().getShortNumbers();
    }

    @RequestMapping(value = "free-numbers/reload", method = RequestMethod.POST)
    public @ResponseBody
    String reloadFreeNumberConfig() {
        OpFreeNumberManager.instance().reload();
        return "ok";
    }

    @RequestMapping(value = "trunk-group/all", method = RequestMethod.POST)
    public @ResponseBody
    List<MTrunkGroup> getConfigureTrunkGroups() {
        return TrunkGroupConfigManager.instance().getAll();
    }

    @RequestMapping(value = "trunk-group/reload", method = RequestMethod.POST)
    public @ResponseBody
    String reloadConfigureTrunkGroups() {
        TrunkGroupConfigManager.instance().reload();
        return "ok";
    }

    @RequestMapping(value = "intl-zone/all", method = RequestMethod.POST)
    public @ResponseBody
    List<MIntlZone> getConfigureInternationalZoneConfig() {
        return IntlZoneConfigManager.instance().getAll();
    }

    @RequestMapping(value = "intl-zone/reload", method = RequestMethod.POST)
    public @ResponseBody
    String reloadConfigureIntelZone() {
        IntlZoneConfigManager.instance().reload();
        return "ok";
    }

    @RequestMapping(value = "lookup/reload", method = RequestMethod.POST)
    public @ResponseBody
    String reloadLookup() {
        reloadLookupCache();
        return "ok";
    }

    private synchronized void reloadLookupCache(){
        try {
            logger.info("Reloading param cache.");
            ServerCache.instance().refreshCache();
            logger.info("Param cache reloaded.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

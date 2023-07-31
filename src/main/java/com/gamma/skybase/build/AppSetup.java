package com.gamma.skybase.build;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.decoder.ascii.DelimitedFileDecoder;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimCbsOfferPayType;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimRoamingPartnerInfo;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimServiceRangeLookup;
import com.gamma.skybase.build.server.etl.decoder.ReferenceDimSuscriberCRMInf;
import com.gamma.telco.utility.reference.*;
import com.gamma.telco.utility.reference.loader.IReferenceLoader;
import com.gamma.telco.utility.reference.loader.MongoReferenceLoader;
import com.gamma.telco.utility.reference.loader.OracleReferenceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.gamma.components.commons.AppUtility.transformFromUnderscoreToCamelCase;
import static com.gamma.components.commons.PlatformUtility.setValue;

public class AppSetup {

    private static AppSetup setup;
    private final Logger logger = LoggerFactory.getLogger(AppSetup.class);
    private final BootSettings settings = BootSettings.instance();

    public static synchronized AppSetup instance() {
        if (setup == null) setup = new AppSetup();
        return setup;
    }

    private AppSetup() {
    }

    public boolean isSetupDone() {
        return settings.isSetupDone();
    }

    public void setup() throws IOException {
        String dbName = AppConfig.instance().getModuleDbProperty("app.module.utility.db");
        IReferenceLoader loader = null;
        switch (dbName.toLowerCase()) {
            case "oracle":
                loader = new OracleReferenceLoader();
                break;
            case "mongo":
                loader = new MongoReferenceLoader();
                break;
            default:
                logger.info("No Suitable Reference Loader class found for dbName : "+dbName);
                break;
        }

        String baseDir = AppConfig.instance().getProperty("app.conf") + File.separator + "business" + File.separator;

        if(loader!= null) {
//            loader.clearContent("DIM_SYSTEM");
//            loader.clearContent("DIM_SERVICE_TYPE");
//            loader.clearContent("DIM_REASON");
//            loader.clearContent("DIM_NETWORK_ELEMENT");
//            loader.clearContent("DIM_EVENT_TYPE");
            loader.clearContent("DIM_DIAL_DIGIT");
//            loader.clearContent("DIM_CELL");
//            loader.clearContent("DIM_BASIC_SERVICE");
//            loader.clearContent("DIM_SUBSCRIPTION");
//            loader.clearContent("DIM_SERVICE_LOOKUP");
//            loader.clearContent("DIM_LOOKUP");
//            loader.clearContent("DIM_SERVICE_CONTEXT");
//            loader.clearContent("DIM_CRM_LOOKUP");
//            loader.clearContent("DIM_SERVICE_CLASS");
//            loader.clearContent("DIM_AIR_SERVICE");
            loader.clearContent("DIM_TADIG_LOOKUP");
            loader.clearContent("DIM_SERVICE_RANGE_LOOKUP_CACHE");
            loader.clearContent("DIM_CRM_INF_SUBSCRIBER_ALL");
            loader.clearContent("DIM_ROAMING_PARTNER_INFO");
            loader.clearContent("DIM_CBS_OFFER_PAYTYPE");

//            loader.loadDimTableContent("DIM_SYSTEM", genericObjectLoad(baseDir + "DIM_SYSTEM.CSV", ReferenceDimSystem.class));
//            loader.loadDimTableContent("DIM_SERVICE_TYPE", genericObjectLoad(baseDir + "DIM_SERVICE_TYPE.CSV", ReferenceDimServiceType.class));
//            loader.loadDimTableContent("DIM_REASON", genericObjectLoad(baseDir + "DIM_REASON.CSV", ReferenceDimReasonType.class));
//            loader.loadDimTableContent("DIM_NETWORK_ELEMENT", genericObjectLoad(baseDir + "DIM_NETWORK_ELEMENT.CSV", ReferenceDimNetworkElement.class));
//            loader.loadDimTableContent("DIM_EVENT_TYPE", genericObjectLoad(baseDir + "DIM_EVENT_TYPE.CSV", ReferenceDimEventType.class));
            loader.loadDimTableContent("DIM_DIAL_DIGIT", genericObjectLoad(baseDir + "DIM_DIAL_DIGIT.CSV", ReferenceDimDialDigit.class));
//            loader.loadDimTableContent("DIM_CELL", SudanCellSiteManager.load());
//            loader.loadDimTableContent("DIM_BASIC_SERVICE", genericObjectLoad(baseDir + "DIM_BASIC_SERVICE.CSV", ReferenceDimBasicService.class));
//            loader.loadDimTableContent("DIM_SUBSCRIPTION", genericObjectLoad(baseDir + "DIM_SUBSCRIPTION.CSV", ReferenceDimSubscription.class));
//            loader.loadDimTableContent("DIM_SERVICE_LOOKUP", genericObjectLoad(baseDir + "DIM_SERVICE_LOOKUP.CSV", ReferenceDimServiceLookup.class));
//            loader.loadDimTableContent("DIM_LOOKUP", genericObjectLoad(baseDir + "DIM_LOOKUP.CSV", ReferenceDimLookup.class));
//            loader.loadDimTableContent("DIM_SERVICE_CONTEXT", genericObjectLoad(baseDir + "DIM_SERVICE_CONTEXT.CSV", ReferenceDimServiceContext.class));
//            loader.loadDimTableContent("DIM_CRM_LOOKUP", genericObjectLoad(baseDir + "DIM_CRM_LOOKUP.csv", ReferenceDimCRMSubscriber.class));
//            loader.loadDimTableContent("DIM_SERVICE_CLASS", genericObjectLoad(baseDir + "DIM_SERVICE_CLASS.csv", ReferenceDimServiceClass.class));
//            loader.loadDimTableContent("DIM_AIR_SERVICE", genericObjectLoad(baseDir + "DIM_AIR_SERVICE.csv", ReferenceDimAirService.class));
            loader.loadDimTableContent("DIM_TADIG_LOOKUP", genericObjectLoad(baseDir + "DIM_TADIG_MAPPING.csv", ReferenceDimTadigLookup.class));
            loader.loadDimTableContent("DIM_SERVICE_RANGE_LOOKUP", genericObjectLoad(baseDir + "DIM_SERVICE_RANGE_LOOKUP.csv", ReferenceDimServiceRangeLookup.class));
            loader.loadDimTableContent("DIM_CRM_INF_SUBSCRIBER_ALL", genericObjectLoad(baseDir + "DIM_CRM_INF_SUBSCRIBER_ALL.csv", ReferenceDimSuscriberCRMInf.class));
            loader.loadDimTableContent("DIM_ROAMING_PARTNER_INFO", genericObjectLoad(baseDir + "DIM_ROAMING_PARTNER_INFO.csv", ReferenceDimRoamingPartnerInfo.class));
            loader.loadDimTableContent("DIM_CBS_OFFER_PAYTYPE", genericObjectLoad(baseDir + "DIM_CBS_OFFER_PAYTYPE.csv", ReferenceDimCbsOfferPayType.class));
            settings.markSetupDone();
        }
    }


    private List<Object> genericObjectLoad(String fileName, Class claz) {
        List<Object> dataset = new ArrayList<>();
        try {
            DelimitedFileDecoder fileDecoder = new DelimitedFileDecoder(fileName);
            while (fileDecoder.hasNext()) {
                Object obj = claz.newInstance();
                Map<String, Object> rec = fileDecoder.next();
                for (Map.Entry<String, Object> entry : rec.entrySet()) {
                    String key = transformFromUnderscoreToCamelCase(entry.getKey().toLowerCase());
                    String value = String.valueOf(entry.getValue());
                    setValue(obj, key, value);
                }
                dataset.add(obj);
            }
        } catch (IOException e) {
            logger.error("Failed to load " + fileName + " mapping file " + e.getMessage(), e);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return dataset;
    }

}

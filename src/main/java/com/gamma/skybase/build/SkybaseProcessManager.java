//package com.gamma.skybase.build;
//
//import com.gamma.components.commons.app.AppConfig;
//import com.gamma.skybase.build.server.service.SkybaseAgentHandler;
//import com.gamma.skybase.server.agents.MAgentCommand;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
///**
// * Created by abhi on 2019-04-29
// */
//@SuppressWarnings("Duplicates")
//public class SkybaseProcessManager {
//
//    private static final Logger logger = LoggerFactory.getLogger(SkybaseProcessManager.class);
//    private static SkybaseProcessManager instance;
//    private List<String> activeDatasources;
//
//    public synchronized static SkybaseProcessManager instance() {
//        if(instance == null) {
//            instance = new SkybaseProcessManager();
//        }
//        return instance;
//    }
//
//    private SkybaseProcessManager(){
//    }
//
//    public void setDatasources(List<String> activeDatasources) {
//        this.activeDatasources = activeDatasources;
//    }
//
//    public void startETL() {
//        if (!activeDatasources.isEmpty()) {
//            SkybaseAgentHandler handler = new SkybaseAgentHandler();
//            MAgentCommand model = new MAgentCommand();
//            model.setCommandName("start");
//
//            for (String datasourceId : activeDatasources) {
//                model.addCommandArguments(datasourceId.trim());
//            }
//
//            logger.info("Going to start skybase manager with following sources : "
//                    + StringUtils.join(model.getCommandArgumentsAsArray(), ","));
//            handler.executeAgentCommand(model);
//        }
//    }
//
//
//    public void stopETL() {
//        if (!activeDatasources.isEmpty()) {
//            SkybaseAgentHandler handler = new SkybaseAgentHandler();
//            MAgentCommand model = new MAgentCommand();
//            model.setCommandName("stop");
//
//            for (String datasourceId : activeDatasources) {
//                model.addCommandArguments(datasourceId.trim());
//            }
//
//            logger.info("Going to stop skybase manager with following sources : " + StringUtils.join(model.getCommandArgumentsAsArray(), ","));
//            handler.executeAgentCommand(model);
//        }
//
//        while (true) {
//            boolean processing = checkIfProcessing();
//            if (!processing) {
//                logger.error("No pending jobs for any adapter");
//                break;
//            } else {
//                try {
//                    TimeUnit.SECONDS.sleep(1);
//                } catch (InterruptedException e) {
//                    logger.error(e.getMessage(), e);
//                }
//            }
//        }
//    }
//
//    public boolean checkIfProcessing() {
//        for (String adapterId : activeDatasources) {
//            SkybaseAgentHandler handler = new SkybaseAgentHandler();
//            MAgentCommand model = new MAgentCommand();
//            model.setCommandName("is_processing");
//            model.addCommandArguments(adapterId);
//            boolean processing = (boolean) handler.executeAgentCommand(model);
//            if (processing) {
//                logger.info("Adapter job is still processing : " + adapterId);
//                return true;
//            }
//        }
//        return false;
//    }
//
//    public  boolean isTerminateSignalReceived() {
//        String terminateSignalFile = AppConfig.instance().getProperty("app.event.terminate.signal-file");
//        return new File(terminateSignalFile).exists();
//    }
//
//    public  void deleteTerminateSignalFile() {
//        String terminateSignalFile = AppConfig.instance().getProperty("app.event.terminate.signal-file");
//        new File(terminateSignalFile).deleteOnExit();
//    }
//
//
//
//
//}

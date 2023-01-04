//package com.gamma.skybase.build;
//
//import com.gamma.components.cache.core.ServerCache;
//import com.gamma.components.commons.app.AppConfig;
//import com.gamma.components.commons.args.ArgsEval;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.builder.SpringApplicationBuilder;
//import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
//import org.springframework.context.ConfigurableApplicationContext;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.event.ContextRefreshedEvent;
//import org.springframework.context.event.EventListener;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
///**
// * Created by abhi on 2019-03-05
// */
//
//@ComponentScan("com.gamma")
//@SpringBootApplication
//public class ApplicationBootstrap extends SpringBootServletInitializer {
//
//    private final static Logger logger = LoggerFactory.getLogger(ApplicationBootstrap.class);
//    private static boolean preBootDone = false;
//    private static boolean postBootDone = false;
//    private static ConfigurableApplicationContext configurableApplicationContext;
//
//    private static SkybaseProcessManager processManager;
//
//
//    public static void postBoot() {
//        if (postBootDone) {
//            logger.warn("Post boot phase already completed. Ignoring request.");
//            return;
//        }
//        logger.info("App loading post-boot methods");
//
//        processManager.startETL();
//
//        postBootDone = true;
//    }
//
//    public static void preBoot() {
//
//        if (preBootDone) {
//            logger.warn("Pre boot phase already completed. Ignoring request.");
//            return;
//        }
//
//        logger.info("App loading pre-boot methods");
//
//        if (AppLicenseValidator.instance().hasExpired()) {
//            String msg = "App license has expired. " +
//                    "Please contact Gamma sales (sales@gammanalytics.com) for renewal " +
//                    "or write to info@gammanalytics.com";
//            logger.error(msg);
//            throw new RuntimeException(msg);
//        }
//
//        if (!AppSetup.instance().isSetupDone()) {
//            logger.info("Loading setup files");
//            AppSetup.instance().setup();
//        }
//
//        ServerCache.instance();
//
//        preBootDone = true;
//    }
//
//    public static void boot() {
//        preBoot();
//        postBoot();
//    }
//
//
//    public static void main(String[] args) throws Exception {
//
//        List<String> activeDatasources = new ArrayList<>();
//        if (args != null && args.length > 0) {
//            logger.info("received args : " + args.length);
//            ArgsEval argsEval = new ArgsEval(args);
//            AppConfig.overrideProperties(argsEval.getArgsKeyValStringMap());
//            argsEval.getArgsKeyValStringMap().forEach((k, v) -> {
//                logger.info(k + " ::: " + v);
//            });
//            String ds = argsEval.getString("app.datasources");
//            if (ds != null) {
//                String[] sources = ds.split("\\|");
//                activeDatasources.addAll(Arrays.asList(sources));
//            }
//        }
//
//        if (activeDatasources.isEmpty()) {
//            activeDatasources.addAll(Arrays.asList("Eric-MSC", "Eric-MSC-Stitcher", "Eric-GMSC",
//                    "Eric-GMSC-Stitcher", "Eric-CCN", "Huawei-GGSN"));
//        }
//
//        processManager = SkybaseProcessManager.instance();
//        processManager.setDatasources(activeDatasources);
//
//        ApplicationBootstrap.preBoot();
//
//        int eventMonitorIntervalInSecs = Integer.parseInt(AppConfig.instance().getProperty("app.event.monitor-interval-in-secs"));
//        new Thread(() -> {
//            while (true) {
//                shutdownEventMonitor();
//                if (processManager.isTerminateSignalReceived()) {
//                    processManager.deleteTerminateSignalFile();
//                    break;
//                }
//                try {
//                    logger.debug("Event monitor going to sleep for : " + eventMonitorIntervalInSecs + " secs");
//                    Thread.sleep(eventMonitorIntervalInSecs * 1_000);
//                } catch (InterruptedException e) {
//                    logger.error(e.getMessage(), e);
//                }
//            }
//            logger.info("Successful application shutdown.");
//            System.exit(0);
//        }).start();
//        configurableApplicationContext = SpringApplication.run(ApplicationBootstrap.class);
//    }
//
//    @Override
//    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
//        return application.sources(ApplicationBootstrap.class);
//    }
//
//    @EventListener
//    public void handleContextRefresh(ContextRefreshedEvent event) {
//        if (!preBootDone) preBoot();
//        if (!postBootDone) postBoot();
//    }
//
//    public static void shutdownEventMonitor() {
//        if (processManager.isTerminateSignalReceived()) {
//            logger.info("Terminate event received. Going to shutdown ...");
//            shutdown();
//        }
//    }
//
//    private static void shutdown() {
//        logger.info("Shutdown request for skybase");
//        processManager.stopETL();
//        configurableApplicationContext.stop();
//    }
//}

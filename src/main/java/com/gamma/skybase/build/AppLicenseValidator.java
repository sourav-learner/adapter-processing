package com.gamma.skybase.build;

import com.gamma.components.commons.app.AppConfig;
import com.gamma.components.license.License;
import com.gamma.components.license.LicenseManager;
import com.gamma.components.license.exception.LicenseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class AppLicenseValidator {

    private final static Logger logger = LoggerFactory.getLogger(AppLicenseValidator.class);

    private static AppLicenseValidator instance;
    private static LicenseManager manager;

    public static synchronized AppLicenseValidator instance() {
        if (instance == null) {
            instance = new AppLicenseValidator();
        }
        return instance;
    }

    private AppLicenseValidator() {
        try {
            manager = new LicenseManager(AppLicenseValidator.class.getResourceAsStream("/pubkey.der"), null);
        } catch (Exception e) {
            throw new LicenseException("Failed while initialising license manager. Abort.");
        }
    }

    public boolean hasExpired() {
        try {
            String licenseFilePath = AppConfig.instance().getProperty("app.license.file-path");
            License license = manager.readLicenseFile(new File(licenseFilePath));
            license.validate(new Date(), null);
            return false;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return true;
        }
    }
}

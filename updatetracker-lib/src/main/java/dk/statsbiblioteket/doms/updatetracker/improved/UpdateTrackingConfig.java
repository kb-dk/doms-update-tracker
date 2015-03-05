package dk.statsbiblioteket.doms.updatetracker.improved;

import java.io.File;
import java.util.Properties;

public class UpdateTrackingConfig {
    private final String fedoraWebUrl;
    private final String fedoraWebUsername;
    private final String fedoraWebPassword;

    private final String fedoraDatabaseDriver;
    private final String fedoraDatabaseURL;
    private final String fedoraDatabaseUsername;
    private final String fedoraDatabasePassword;

    private final int fedoraUpdatetrackerDelay;
    private final int fedoraUpdatetrackerPeriod;
    private final int fedoraUpdatetrackerLimit;
    private final File updatetrackerHibernateConfig;


    public UpdateTrackingConfig(Properties properties) {

        String fedoraWebUrl = properties.getProperty("fedora.web.URL");
        String fedoraWebUsername = properties.getProperty("fedora.web.Username");
        String fedoraWebPassword = properties.getProperty("fedora.web.Password");

        String fedoraDatabaseDriver = properties.getProperty("fedora.database.driver");
        String fedoraDatabaseURL = properties.getProperty("fedora.database.URL");
        String fedoraDatabaseUsername = properties.getProperty("fedora.database.username");
        String fedoraDatabasePassword = properties.getProperty("fedora.database.password");

        int fedoraUpdatetrackerDelay = Integer.parseInt(properties.getProperty("fedora.updatetracker.delay"));
        int fedoraUpdatetrackerPeriod = Integer.parseInt(properties.getProperty("fedora.updatetracker.period"));
        int fedoraUpdatetrackerLimit = Integer.parseInt(properties.getProperty("fedora.updatetracker.limit"));
        File updatetrackerHibernateConfig = new File(properties.getProperty("fedora.updatetracker.hibernateConfigFile"));

        this.fedoraWebUrl = fedoraWebUrl;
        this.fedoraWebUsername = fedoraWebUsername;
        this.fedoraWebPassword = fedoraWebPassword;
        this.fedoraDatabaseDriver = fedoraDatabaseDriver;
        this.fedoraDatabaseURL = fedoraDatabaseURL;
        this.fedoraDatabaseUsername = fedoraDatabaseUsername;
        this.fedoraDatabasePassword = fedoraDatabasePassword;
        this.fedoraUpdatetrackerDelay = fedoraUpdatetrackerDelay;
        this.fedoraUpdatetrackerPeriod = fedoraUpdatetrackerPeriod;
        this.fedoraUpdatetrackerLimit = fedoraUpdatetrackerLimit;
        this.updatetrackerHibernateConfig = updatetrackerHibernateConfig;
    }


    public UpdateTrackingConfig(String fedoraWebUrl, String fedoraWebUsername, String fedoraWebPassword,
                                String fedoraDatabaseDriver, String fedoraDatabaseURL, String fedoraDatabaseUsername, String fedoraDatabasePassword, int fedoraUpdatetrackerDelay, int fedoraUpdatetrackerPeriod,
                                int fedoraUpdatetrackerLimit, File updatetrackerHibernateConfig) {
        this.fedoraWebUrl = fedoraWebUrl;
        this.fedoraWebUsername = fedoraWebUsername;
        this.fedoraWebPassword = fedoraWebPassword;
        this.fedoraDatabaseDriver = fedoraDatabaseDriver;
        this.fedoraDatabaseURL = fedoraDatabaseURL;
        this.fedoraDatabaseUsername = fedoraDatabaseUsername;
        this.fedoraDatabasePassword = fedoraDatabasePassword;
        this.fedoraUpdatetrackerDelay = fedoraUpdatetrackerDelay;
        this.fedoraUpdatetrackerPeriod = fedoraUpdatetrackerPeriod;
        this.fedoraUpdatetrackerLimit = fedoraUpdatetrackerLimit;
        this.updatetrackerHibernateConfig = updatetrackerHibernateConfig;
    }

    public String getFedoraWebUrl() {
        return fedoraWebUrl;
    }

    public String getFedoraWebUsername() {
        return fedoraWebUsername;
    }

    public String getFedoraWebPassword() {
        return fedoraWebPassword;
    }

    public String getFedoraDatabaseURL() {
        return fedoraDatabaseURL;
    }

    public String getFedoraDatabaseUsername() {
        return fedoraDatabaseUsername;
    }

    public String getFedoraDatabasePassword() {
        return fedoraDatabasePassword;
    }

    public int getFedoraUpdatetrackerDelay() {
        return fedoraUpdatetrackerDelay;
    }

    public int getFedoraUpdatetrackerPeriod() {
        return fedoraUpdatetrackerPeriod;
    }

    public int getFedoraUpdatetrackerLimit() {
        return fedoraUpdatetrackerLimit;
    }

    public String getFedoraDatabaseDriver() {
        return fedoraDatabaseDriver;
    }

    public File getUpdatetrackerHibernateConfig() {
        return updatetrackerHibernateConfig;
    }

}

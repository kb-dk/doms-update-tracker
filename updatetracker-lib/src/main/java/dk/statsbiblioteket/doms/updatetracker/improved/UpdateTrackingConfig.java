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
    private final String fedoraUpdatetrackerProgressFile;


    /**
     * Create a Config object from a java properties. Will look for these keys which are required
     * fedora.updatetracker.web.URL
     * fedora.updatetracker.web.username
     * fedora.updatetracker.web.password
     * fedora.worklog.database.driver
     * fedora.worklog.database.URL
     * fedora.worklog.database.username
     * fedora.worklog.database.password
     * fedora.updatetracker.progressFile = /tmp/updateTracker.progress
     * fedora.updatetracker.hibernateConfigFile
     *
     * These properties are optional, and the default value is mentioned
     * fedora.updatetracker.delay The delay in starting polling the worklog when starting this service, in milliseconds. Default 1000ms
     * fedora.updatetracker.period The frequency in milliseconds, which the worklog should be polled. Default 1000ms
     * fedora.updatetracker.limit The pagesize to get from the worklog. Default 1000
     *
     * As it stands, the listener will take at most limit workunits from the worklog, and play these against the update
     * tracker for each period. That means that if doms, on average, do more than limit / period (operations / second), the
     * update tracker will never catch up.
     * @param properties
     */
    public UpdateTrackingConfig(Properties properties) {

        this.fedoraWebUrl = properties.getProperty("fedora.updatetracker.web.URL");
        this.fedoraWebUsername = properties.getProperty("fedora.updatetracker.web.username");
        this.fedoraWebPassword = properties.getProperty("fedora.updatetracker.web.password");
        this.fedoraDatabaseDriver = properties.getProperty("fedora.worklog.database.driver");
        this.fedoraDatabaseURL = properties.getProperty("fedora.worklog.database.URL");
        this.fedoraDatabaseUsername = properties.getProperty("fedora.worklog.database.username");
        this.fedoraDatabasePassword = properties.getProperty("fedora.worklog.database.password");
        this.fedoraUpdatetrackerDelay = Integer.parseInt(properties.getProperty("fedora.updatetracker.delay","1000"));
        this.fedoraUpdatetrackerPeriod = Integer.parseInt(properties.getProperty("fedora.updatetracker.period","1000"));
        this.fedoraUpdatetrackerLimit = Integer.parseInt(properties.getProperty("fedora.updatetracker.limit","1000"));
        this.fedoraUpdatetrackerProgressFile = properties.getProperty("fedora.updatetracker.progressFile","/tmp/updateTracker.progress");
        this.updatetrackerHibernateConfig = new File(properties.getProperty("fedora.updatetracker.hibernateConfigFile"));
    }


    public UpdateTrackingConfig(String fedoraWebUrl, String fedoraWebUsername, String fedoraWebPassword,
                                String fedoraDatabaseDriver, String fedoraDatabaseURL, String fedoraDatabaseUsername,
                                String fedoraDatabasePassword, int fedoraUpdatetrackerDelay, int fedoraUpdatetrackerPeriod,
                                int fedoraUpdatetrackerLimit, File updatetrackerHibernateConfig,
                                String fedoraUpdatetrackerProgressFile) {
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
        this.fedoraUpdatetrackerProgressFile = fedoraUpdatetrackerProgressFile;
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

    public String getFedoraUpdatetrackerProgressFile() {
        return fedoraUpdatetrackerProgressFile;
    }
}

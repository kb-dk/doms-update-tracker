package dk.statsbiblioteket.doms.updatetracker.improved;

import java.io.File;
import java.util.Properties;

public class UpdateTrackingConfig {
    /**
     * URL to fedora web frontend, ala http://localhost:7880/fedora/objects
     */
    protected static final String FEDORA_UPDATETRACKER_WEB_URL = "fedora.updatetracker.web.URL";
    /**
     * Username to the fedora web frontend. Do not need to change stuff, but must be able to read all
     * @see #FEDORA_UPDATETRACKER_WEB_PASSWORD
     */
    protected static final String FEDORA_UPDATETRACKER_WEB_USERNAME = "fedora.updatetracker.web.username";
    /**
     * Password to the fedora web frontend. Do not need to change stuff, but must be able to read all
     * * @see #FEDORA_UPDATETRACKER_WEB_USERNAME
     */
    protected static final String FEDORA_UPDATETRACKER_WEB_PASSWORD = "fedora.updatetracker.web.password";
    /**
     * The database driver to use when talking the worklog database
     * @see #FEDORA_WORKLOG_DATABASE_URL
     */
    protected static final String FEDORA_WORKLOG_DATABASE_DRIVER = "fedora.worklog.database.driver";
    /**
     * The jdbc url to fedoras fieldSearchDB, where the worklog is stored per default.
     */
    protected static final String FEDORA_WORKLOG_DATABASE_URL = "fedora.worklog.database.URL";
    /**
     * Readonly account to the fedora worklog database
     * @see #FEDORA_WORKLOG_DATABASE_URL
     */
    protected static final String FEDORA_WORKLOG_DATABASE_USERNAME = "fedora.worklog.database.username";
    /**
     * Readonly account to the fedora worklog database
     * @see #FEDORA_WORKLOG_DATABASE_URL
     */
    protected static final String FEDORA_WORKLOG_DATABASE_PASSWORD = "fedora.worklog.database.password";
    /**
     * The delay in starting polling the worklog when starting this service, in
     * milliseconds. Default 1000ms
     */
    protected static final String FEDORA_UPDATETRACKER_DELAY = "fedora.updatetracker.delay";
    /**
     * The frequency in milliseconds, which the worklog should be polled. Default 1000ms
     */
    protected static final String FEDORA_UPDATETRACKER_PERIOD = "fedora.updatetracker.period";
    /**
     * The pagesize to get from the worklog. Default 1000
     */
    protected static final String FEDORA_UPDATETRACKER_LIMIT = "fedora.updatetracker.limit";
    /**
     * Address of the file to store progress information, i.e. the worklog latest key
     */
    protected static final String FEDORA_UPDATETRACKER_PROGRESS_FILE = "fedora.updatetracker.progressFile";
    /**
     * The hibernate config file for the update tracker database
     */
    protected static final String FEDORA_UPDATETRACKER_HIBERNATE_CONFIG_FILE
            = "fedora.updatetracker.hibernateConfigFile";
    /**
     * The time a view bundle should remain cached. This is solely a memory issue, as the cache keys involve the view bundle timestamp
     */
    private static final java.lang.String FEDORA_UPDATETRACKER_VIEWBUNDLE_CACHETIME
            = "fedora.updatetracker.viewbundleCacheTime";
    /**
     * The hibernate mappings file for the update tracker database. It is this file that defines the indexes we use
     */
    private static final java.lang.String FEDORA_UPDATETRACKER_HIBERNATE_MAPPINGS_FILE
            = "fedora.updatetracker.hibernateConfigFile";
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
    private final long viewBundleCacheTime;
    private File updatetrackerHibernateMappings;


    /**
     * Create a Config object from a java properties.
     *
     * As it stands, the listener will take at most "limit" workunits from the worklog, and play these against the update tracker for each "period". That means that if doms, on average, do more than limit / period (operations / second), the update tracker will never catch up.
     * @param properties
     * @see #FEDORA_UPDATETRACKER_WEB_URL
     * @see #FEDORA_UPDATETRACKER_WEB_USERNAME
     * @see #FEDORA_WORKLOG_DATABASE_PASSWORD
     * @see #FEDORA_WORKLOG_DATABASE_DRIVER
     * @see #FEDORA_WORKLOG_DATABASE_URL
     * @see #FEDORA_WORKLOG_DATABASE_USERNAME
     * @see #FEDORA_WORKLOG_DATABASE_PASSWORD
     * @see #FEDORA_UPDATETRACKER_HIBERNATE_CONFIG_FILE
     * @see #FEDORA_UPDATETRACKER_DELAY
     * @see #FEDORA_UPDATETRACKER_PERIOD
     * @see #FEDORA_UPDATETRACKER_LIMIT
     */
    public UpdateTrackingConfig(Properties properties) {

        this.fedoraWebUrl = properties.getProperty(FEDORA_UPDATETRACKER_WEB_URL);
        this.fedoraWebUsername = properties.getProperty(FEDORA_UPDATETRACKER_WEB_USERNAME);
        this.fedoraWebPassword = properties.getProperty(FEDORA_UPDATETRACKER_WEB_PASSWORD);
        this.fedoraDatabaseDriver = properties.getProperty(FEDORA_WORKLOG_DATABASE_DRIVER);
        this.fedoraDatabaseURL = properties.getProperty(FEDORA_WORKLOG_DATABASE_URL);
        this.fedoraDatabaseUsername = properties.getProperty(FEDORA_WORKLOG_DATABASE_USERNAME);
        this.fedoraDatabasePassword = properties.getProperty(FEDORA_WORKLOG_DATABASE_PASSWORD);
        this.fedoraUpdatetrackerDelay = Integer.parseInt(properties.getProperty(FEDORA_UPDATETRACKER_DELAY, "1000"));
        this.fedoraUpdatetrackerPeriod = Integer.parseInt(properties.getProperty(FEDORA_UPDATETRACKER_PERIOD,"1000"));
        this.fedoraUpdatetrackerLimit = Integer.parseInt(properties.getProperty(FEDORA_UPDATETRACKER_LIMIT,"1000"));
        this.updatetrackerHibernateConfig = new File(properties.getProperty(FEDORA_UPDATETRACKER_HIBERNATE_CONFIG_FILE));
        this.updatetrackerHibernateMappings = new File(properties
                                                             .getProperty(FEDORA_UPDATETRACKER_HIBERNATE_MAPPINGS_FILE));
        this.viewBundleCacheTime = Long.parseLong(properties.getProperty(FEDORA_UPDATETRACKER_VIEWBUNDLE_CACHETIME,"10000"));
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

    public Long getViewBundleCacheTime() {
        return viewBundleCacheTime;
    }

    public File getUpdatetrackerHibernateMappings() {
        return updatetrackerHibernateMappings;
    }
}

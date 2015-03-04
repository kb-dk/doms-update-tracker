package dk.statsbiblioteket.doms.updatetracker.improved;

import java.io.File;

public class UpdateTrackingConfig {
    private final String fedoraLocation;
    private final String fedoraUser;
    private final String fedoraPass;

    private final String fedoraDatabaseDriver;
    private final String fedoraDatabaseUrl;
    private final String fedoraDatabaseUser;
    private final String fedoraDatabasePassword;

    private final int delay;
    private final int period;
    private final int limit;
    private final File hibernateConfigFile;


    public UpdateTrackingConfig(String fedoraLocation, String fedoraUser, String fedoraPass,
                                String fedoraDatabaseDriver, String fedoraDatabaseUrl, String fedoraDatabaseUser, String fedoraDatabasePassword, int delay, int period,
                                int limit, File hibernateConfigFile) {
        this.fedoraLocation = fedoraLocation;
        this.fedoraUser = fedoraUser;
        this.fedoraPass = fedoraPass;
        this.fedoraDatabaseDriver = fedoraDatabaseDriver;
        this.fedoraDatabaseUrl = fedoraDatabaseUrl;
        this.fedoraDatabaseUser = fedoraDatabaseUser;
        this.fedoraDatabasePassword = fedoraDatabasePassword;
        this.delay = delay;
        this.period = period;
        this.limit = limit;
        this.hibernateConfigFile = hibernateConfigFile;
    }

    public String getFedoraLocation() {
        return fedoraLocation;
    }

    public String getFedoraUser() {
        return fedoraUser;
    }

    public String getFedoraPass() {
        return fedoraPass;
    }

    public String getFedoraDatabaseUrl() {
        return fedoraDatabaseUrl;
    }

    public String getFedoraDatabaseUser() {
        return fedoraDatabaseUser;
    }

    public String getFedoraDatabasePassword() {
        return fedoraDatabasePassword;
    }

    public int getDelay() {
        return delay;
    }

    public int getPeriod() {
        return period;
    }

    public int getLimit() {
        return limit;
    }

    public String getFedoraDatabaseDriver() {
        return fedoraDatabaseDriver;
    }

    public File getHibernateConfigFile() {
        return hibernateConfigFile;
    }

}

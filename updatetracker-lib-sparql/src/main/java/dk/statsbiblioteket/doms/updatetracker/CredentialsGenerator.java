package dk.statsbiblioteket.doms.updatetracker;

import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

/**
 * Get credentials from credential source.
 */
public interface CredentialsGenerator {
    public Credentials getCredentials();
}

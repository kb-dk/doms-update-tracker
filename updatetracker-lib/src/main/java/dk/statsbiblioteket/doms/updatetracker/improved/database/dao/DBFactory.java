package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import dk.statsbiblioteket.doms.updatetracker.improved.database.SetLastModifiedInterceptor;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.cfg.Configuration;

import java.io.Closeable;
import java.io.File;

/**
 * Factory for the Database abstraction classes
 */
public class DBFactory implements Closeable{

    private final SessionFactory sessionFactory;

    /**
     * Create a new factory, from a hibernate config file and a file with hibernate mappings
     * @param configFile the hibernate config file
     * @param hibernateMappings the hibernate mappings file, can  be null if no mappings
     */
    public DBFactory(File configFile, File hibernateMappings) {
        // A SessionFactory is set up once for an application
        final Configuration configuration = new Configuration()
                                                    .configure(configFile);
        if (hibernateMappings != null) {
            configuration.addFile(hibernateMappings);
        }
        configuration.setInterceptor(new SetLastModifiedInterceptor());
        sessionFactory = configuration.buildSessionFactory();
    }

    /**
     * Get a read/write database abstraction
     * @return the database abstraction instance
     */
    public DB createDBConnection() {
        return new DB(sessionFactory,false);
    }

    /**
     * Get a read-only database abstraction
     * @return the database abstraction instance
     */
    public DB createReadonlyDBConnection(){
        return new DB(sessionFactory,true);
    }


    @Override
    public void close() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

}

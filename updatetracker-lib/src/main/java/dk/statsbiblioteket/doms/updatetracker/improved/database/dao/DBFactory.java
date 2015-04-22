package dk.statsbiblioteket.doms.updatetracker.improved.database.dao;

import dk.statsbiblioteket.doms.updatetracker.improved.database.SetLastModifiedInterceptor;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.cfg.Configuration;

import java.io.Closeable;
import java.io.File;

public class DBFactory implements Closeable{

    private final SessionFactory sessionFactory;

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

    public DB getInstance() {
        return new DB(sessionFactory);
    }
    public StatelessDB getStatelessDB(){
        return new StatelessDB(sessionFactory);
    }


    @Override
    public void close() {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

}

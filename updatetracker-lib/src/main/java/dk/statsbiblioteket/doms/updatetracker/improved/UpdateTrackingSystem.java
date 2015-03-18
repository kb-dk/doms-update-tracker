package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.central.connectors.fedora.Fedora;
import dk.statsbiblioteket.doms.central.connectors.fedora.FedoraRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.tripleStore.TripleStoreRest;
import dk.statsbiblioteket.doms.central.connectors.fedora.views.ViewsImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerBackend;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStoreImpl;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.EntryAngleCache;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraForUpdateTracker;
import dk.statsbiblioteket.doms.updatetracker.improved.worklog.WorkLogPoller;
import dk.statsbiblioteket.doms.updatetracker.improved.worklog.WorkLogPollTask;
import dk.statsbiblioteket.sbutil.webservices.authentication.Credentials;

import java.io.Closeable;
import java.util.Timer;

/**
 * This is the system that starts the persistent store and the jms listener and ties them together
 */
public class UpdateTrackingSystem implements Closeable {


    private  UpdateTrackerPersistentStore store;
    private WorkLogPoller consumer;
    private  Timer timer;


    private static UpdateTrackingSystem instance = null;

    protected UpdateTrackingSystem(UpdateTrackingConfig updateTrackingConfig) {


        try {
            Credentials creds = new Credentials(updateTrackingConfig.getFedoraWebUsername(),
                                                updateTrackingConfig.getFedoraWebPassword());
            EntryAngleCache cmCache = new EntryAngleCache();
            Fedora fedoraRest = new FedoraRest(creds, updateTrackingConfig.getFedoraWebUrl());
            TripleStoreRest tripleStoreRest = new TripleStoreRest(creds,
                                                                  updateTrackingConfig.getFedoraWebUrl(), fedoraRest);
            ViewsImpl views = new ViewsImpl(tripleStoreRest, fedoraRest);


            //Start up the fedora connection
            FedoraForUpdateTracker fedora = new FedoraForUpdateTracker(cmCache, fedoraRest, tripleStoreRest, views);

            //Start up the database
            final UpdateTrackerBackend updateTrackerBackend = new UpdateTrackerBackend(fedora);
            store = new UpdateTrackerPersistentStoreImpl(updateTrackingConfig.getUpdatetrackerHibernateConfig(), fedora,
                                                         updateTrackerBackend);


            //initialise the jms connection to Fedora
            consumer = new WorkLogPoller(updateTrackingConfig.getFedoraDatabaseDriver(), updateTrackingConfig.getFedoraDatabaseURL(),
                                                updateTrackingConfig.getFedoraDatabaseUsername(),
                                                updateTrackingConfig.getFedoraDatabasePassword());

            final boolean isDaemon = false;
            timer = new Timer(isDaemon);
            //Tie it all together
            final int delay = updateTrackingConfig.getFedoraUpdatetrackerDelay();
            final int period = updateTrackingConfig.getFedoraUpdatetrackerPeriod();
            final int limit = updateTrackingConfig.getFedoraUpdatetrackerLimit();
            timer.schedule(new WorkLogPollTask(consumer, store, limit),
                           delay,
                           period);
        } catch (Exception e){
            close();
            throw new RuntimeException(e);
        }
    }

    public synchronized static UpdateTrackingSystem getInstance(UpdateTrackingConfig updateTrackingConfig) {
        if (instance == null) {
            instance = new UpdateTrackingSystem(updateTrackingConfig);
        }
        return instance;

    }

    @Override
    public void close()  {
        if (timer != null) {
            timer.cancel();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (store != null) {
            store.close();
        }
    }

    public UpdateTrackerPersistentStore getStore() {
        return store;
    }
}

package dk.statsbiblioteket.doms.updatetracker.improved.webservice;

import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingConfig;
import dk.statsbiblioteket.doms.updatetracker.improved.UpdateTrackingSystem;
import dk.statsbiblioteket.sbutil.webservices.configuration.ConfigCollection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

/**
 * This is a loadOnStartup Service. It served nothing, and should probably be a Listener
 * It starts the update tracker system up, when the servlet is loaded. It reads the config from the servlet configuraiton
 * ie. context params. It closes the update tracker when the system is closed
 */
public class UpdateTrackerTimerServlet extends HttpServlet {

    public static UpdateTrackingSystem updateTracker;

    @Override
    public void init() throws ServletException {
        super.init();
        UpdateTrackingConfig config = new UpdateTrackingConfig(ConfigCollection.getProperties());
        updateTracker = new UpdateTrackingSystem(config);
    }

    @Override
    public void destroy() {
        super.destroy();
        updateTracker.close();
    }
}

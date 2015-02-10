package dk.statsbiblioteket.doms.updatetracker.improved.database;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.Fedora;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.ViewInfo;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 5/3/11
 * Time: 11:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class FedoraMockup extends Fedora{

    Set<String> objects = new HashSet<String>();

    Map<String,Set<String>> bundles = new HashMap<String,Set<String>>();

    public FedoraMockup(Credentials creds, String fedoraLocation, String ecmLocation) throws MalformedURLException {
        super(creds,"http://example.com");
    }

    protected void addObject(String pid){
        objects.add(pid);
    }

    protected void addEntry(String entrypid, String... objects){
        HashSet<String> contained = new HashSet<String>(Arrays.asList(objects));
        contained.add(entrypid);
        bundles.put(entrypid, contained);
        addObject(entrypid);
        for (String object : objects) {
            addObject(object);
        }
    }

    @Override
    public List<ViewInfo> getViewInfo(String pid, Date date) {
        if (bundles.containsKey(pid)){
            ArrayList<ViewInfo> list = new ArrayList<ViewInfo>();
            ViewInfo view = new ViewInfo("SummaVisible",true,pid);
            list.add(view);
            return list;

        }
        else {
            return new ArrayList<ViewInfo>();
        }
    }

    @Override
    public ViewBundle calcViewBundle(String entryPid, String viewAngle, Date date) {
        if (bundles.containsKey(entryPid)){
            ViewBundle bundle = new ViewBundle(entryPid,viewAngle);
            bundle.setContained(new ArrayList<>(bundles.get(entryPid)));
            return bundle;
        }
        return null;
    }

    @Override
    public Set<String> getCollections(String pid, Date date) throws FedoraFailedException {
        return new HashSet<>(Arrays.asList("doms:Root_Collection"));
    }

    @Override
    public List<ViewInfo> getContentModelViewInfo(String pid, Date date) throws FedoraFailedException {
        return super.getContentModelViewInfo(pid, date);
    }
}

package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import dk.statsbiblioteket.doms.updatetracker.improved.database.ViewBundle;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ObjectFactory;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ObjectProfile;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewangleType;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.generated.ViewsType;
import dk.statsbiblioteket.doms.webservices.authentication.Base64;
import dk.statsbiblioteket.doms.webservices.authentication.Credentials;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/28/11
 * Time: 12:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class Fedora {

    private static Client client = Client.create();
    private WebResource restApi;
    private WebResource ecmApi;
    private Credentials creds;
    private WebResource risearchApi;

    public Fedora(Credentials creds, String fedoraLocation, String ecmLocation)
            throws MalformedURLException {
        this.creds = creds;
        restApi = client.resource(fedoraLocation + "/objects/");
        risearchApi = client.resource(fedoraLocation + "/risearch/");
        ecmApi = client.resource(ecmLocation);
    }


    public List<ViewInfo> getViewInfo(String pid, Date date) {

        Map<String,List<String>> relations = new HashMap<String,List<String>>();
        Map<String, List<String>> inverseRelations = new HashMap<String,List<String>>();

        Set<String> entryAngles = determineEntryAngles(pid);

        Set<String> angles = new HashSet<String>();
        angles.addAll(entryAngles);


        ObjectProfile profile = restApi.path(pid)
                .queryParam("asOfDateTime", DateUtility.convertDateToString(date))
                .queryParam("format", "xml")
                .header("Authorization", credsAsBase64())
                .get(ObjectProfile.class);

        List<String> contentmodels = profile.getObjModels().getModel();


        for (String contentmodel : contentmodels) {
            ViewsType viewStream = restApi.path(contentmodel).path("/datastreams/VIEW")
                    .queryParam("asOfDateTime", DateUtility.convertDateToString(date))
                    .get(ViewsType.class);
            for (ViewangleType viewangleType : viewStream.getViewangle()) {
                String name = viewangleType.getName();
                angles.add(name);

                List<String> rels = relations.get(name);
                if (rels == null){
                    rels = new ArrayList<String>();
                }
                List<Object> markedRels = viewangleType.getRelations().getAny();
                for (Object markedRel : markedRels) {
                    rels.add(markedRel.toString());
                }
                relations.put(name,rels);

                List<String> invrels = relations.get(name);
                if (invrels == null){
                    invrels = new ArrayList<String>();
                }
                List<Object> markedInvRels = viewangleType.getInverseRelations().getAny();
                for (Object markedInvRel : markedInvRels) {
                    rels.add(markedInvRel.toString());
                }
                inverseRelations.put(name,rels);
            }
        }

        List<ViewInfo> infoList = new ArrayList<ViewInfo>();
        for (String angle : angles) {
            ViewInfo info = new ViewInfo(angle, pid);
            info.setEntry(entryAngles.contains(angle));
            info.setRelations(relations.get(angle));
            info.setInverseRelations(inverseRelations.get(angle));
            infoList.add(info);
        }
        return infoList;


    }

    private Set<String> determineEntryAngles(String pid) {
        Set<String> angles = new HashSet<String>();
        String query = "select $angle\n" +
                "from <#ri>\n" +
                "where \n" +
                "<info:fedora/"+pid+"> <fedora-model:hasModel> $cm\n" +
                "and\n" +
                "$cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> $angle";

        String anglesString = risearchApi
                .queryParam("type", "tuples")
                .queryParam("lang", "iTQL")
                .queryParam("format", "CSV")
                .queryParam("flush", "true")
                .queryParam("stream", "on")
                .queryParam("query", query)
                .header("Authorization", credsAsBase64())
                .post(String.class);

        String[] lines = anglesString.split("\n");
        for (String line : lines) {
            if (line.startsWith("\"")){
                continue;
            }

            line = line.replaceAll("info:fedora/","");

            angles.add(line);
        }
        return angles;
    }

    public ViewBundle calcViewBundle(String entryPid, String viewAngle, Date date) {


        PidList pids = ecmApi.path("getViewObjectsForObject/")
                .path(entryPid)
                .path("/forAngle/")
                .path(viewAngle)
                .queryParam("bundle", "false")
                .header("Authorization", credsAsBase64()).get(PidList.class);


        ViewBundle bundle = new ViewBundle(entryPid,viewAngle,pids);
        return bundle;
    }


    public List<ObjectInfo> getAllEntryObjects(){
        String query = "select $pid $state $lastModified $angle\n" +
                "from <#ri>\n" +
                "where\n" +
                "$pid <fedora-model:state> $state\n" +
                "and\n" +
                "$pid <fedora-model:hasModel> $cm\n" +
                "and\n" +
                "$cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> $angle\n" +
                "and\n" +
                "$pid <fedora-view:lastModifiedDate> $lastModified\n" +
                "order by $lastModified";

        ClientResponse response = risearchApi
                .queryParam("type", "tuples")
                .queryParam("lang", "iTQL")
                .queryParam("format", "CSV")
                .queryParam("flush", "true")
                .queryParam("stream", "on")
                .queryParam("query", query)
                .header("Authorization", credsAsBase64())
                .post(ClientResponse.class);
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntityInputStream()));
        //TODO check return code

        ArrayList<ObjectInfo> result = new ArrayList<ObjectInfo>();
        try {
            String line = reader.readLine();//discard first line
            ObjectInfo objectInfo = null;

            while ((line = reader.readLine()) != null){
                String[] splits = line.split("\\s");
                //TODO fix for info:fedora
                String pid = splits[0];
                String state = splits[1];
                String lastModified = splits[2];
                String viewAngle = splits[3];
                if (objectInfo != null && objectInfo.getObjectPid().equals(pid)){
                    objectInfo.add(viewAngle);
                } else {
                    objectInfo = new ObjectInfo();
                    objectInfo.setObjectPid(pid);
                    //TODO parse date
                    objectInfo.setLastModified(DateUtility.convertStringToDate(lastModified));
                    objectInfo.setState(state);
                    objectInfo.add(viewAngle);
                    result.add(objectInfo);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return result;
    }



    protected String credsAsBase64(){
        String preBase64 = creds.getUsername() + ":" + creds.getPassword();
        String base64 = Base64.encodeBytes(preBase64.getBytes());
        return "Basic "+base64;
    }



}


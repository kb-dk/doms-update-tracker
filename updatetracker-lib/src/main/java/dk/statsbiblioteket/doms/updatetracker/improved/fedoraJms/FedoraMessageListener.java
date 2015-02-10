package dk.statsbiblioteket.doms.updatetracker.improved.fedoraJms;

import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerPersistentStore;
import dk.statsbiblioteket.doms.updatetracker.improved.database.UpdateTrackerStorageException;
import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;
import dk.statsbiblioteket.doms.updatetracker.improved.jms.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.xml.bind.*;
import java.io.StringReader;
import java.util.Date;
import java.util.List;

/**
 * This is the System for listening to fedora JMS ATOM messages
 */
public class FedoraMessageListener implements MessageListener{
    private UpdateTrackerPersistentStore store;

    public FedoraMessageListener(
            UpdateTrackerPersistentStore store) {
        this.store = store;
    }


    /**
     * Listing method
     * @param message The fedora message
     */
    @Override
    public void onMessage(Message message) {
        TextMessage msg;
        if (message instanceof TextMessage) {
            msg = (TextMessage) message;
            onMessage(msg);
        } else {
            System.out.println("Message is not a TextMessage");
        }
    }

    public void onMessage(TextMessage message) {
        try {
            String atomblub = message.getText();
            EntryType entry = JAXB.unmarshal(new StringReader(atomblub), EntryType.class);
            String method = getFedoraMethod(entry);

            switch (method) {
                case "ingest" :
                    objectCreated(entry);
                    break;
                case "modifyObject" :
                    objectModified(entry);
                    break;
                case "purgeObject" :
                    objectDeleted(entry);
                    break;
                case "addDatastream" :
                case "modifyDatastreamByReference" :
                case "modifyDatastreamByValue" :
                case "purgeDatastream" :
                case "setDatastreamState" :
                case "setDatastreamVersionable" :
                    datastreamModified(entry);
                    break;
                case "addRelationship" :
                case "purgeRelationship" :
                    relationsChanged(entry);
                    break;
                case "getObjectXML" :
                case "export" :
                case "getDatastream" :
                case "getDatastreams" :
                case "getDatastreamHistory" :
                case "putTempStream" :
                case "getTempStream" :
                case "compareDatastreamChecksum" :
                case "getNextPID" :
                case "getRelationships" :
                case "validate" :
                    // Nothing to do
                    break;
                default:
                    // TODO log this
                    break;
            }
        } catch (JMSException | UpdateTrackerStorageException | FedoraFailedException e) {
            //TODO log this - or handle and retry?
            e.printStackTrace();
        }

    }


    private void objectCreated(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        String pid = getPid(entry);
        Date date = getDate(entry);
        store.objectCreated(pid, date);

    }

    private void objectDeleted(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        String pid = getPid(entry);
        Date date = getDate(entry);
        store.objectDeleted(pid, date);
    }

    private void relationsChanged(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        String pid = getPid(entry);
        Date date = getDate(entry);
        store.objectRelationsChanged(pid, date);
    }

    private void objectModified(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {

        String pid = getPid(entry);
        Date date = getDate(entry);
        String newstate = null;
        newstate = getNewState(entry);
        if (newstate != null) {
            if (newstate.equals("D")) {

            } else if (newstate != null) {
                store.objectStateChanged(pid, date, newstate);
            }
        }  else {

        }
    }


    private void datastreamModified(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        String pid = getPid(entry);
        Date date = getDate(entry);
        String dsid = getDatastream(entry);
        if (pid != null && date != null && dsid != null) {
            store.datastreamChanged(pid, date, dsid);
        }
    }


    private String getFedoraMethod(EntryType entry) {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();

        String method = "";

        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;

                if (jaxbElement.getDeclaredType().isAssignableFrom(TextType.class)) {//text types
                    TextType text = (TextType) jaxbElement.getValue();
                    String name = jaxbElement.getName().getLocalPart();
                    if (name.equals("title")) {
                        method = text.getContent().toString().replaceAll("[\\[\\]]", "");
                    }
                }
            }
        }
        return method;
    }


    private String getPid(EntryType entry) {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();
        String pid = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;
                if (jaxbElement.getDeclaredType().isAssignableFrom(ContentType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("content")) {
                        ContentType content = (ContentType) jaxbElement.getValue();
                        pid = content.getContent().get(0).toString();
                    }
                }
            }
        }
        return pid;
    }

    private Date getDate(EntryType entry) {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();
        Date date  = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;
                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

            }
        }
        return date;
    }



    private String getDatastream(EntryType entry) {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();
        String dsid = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;
                if (jaxbElement.getDeclaredType().isAssignableFrom(CategoryType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("category")) {
                        CategoryType  category = (CategoryType) jaxbElement.getValue();
                        if (category.getScheme().equals("fedora-types:dsID")){
                            dsid = category.getTerm();
                        }
                    }
                }
            }
        }
        return dsid;
    }


    private String getNewState(EntryType entry) {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();
        String newstate = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;
                if (jaxbElement.getDeclaredType().isAssignableFrom(CategoryType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("category")) {
                        CategoryType  category = (CategoryType) jaxbElement.getValue();
                        if (category.getScheme().equals("fedora-types:state")){
                            newstate = category.getTerm();
                        }
                    }
                }
            }
        }
        return newstate;
    }
}

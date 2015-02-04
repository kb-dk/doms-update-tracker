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
            List<Object> properties = entry.getAuthorOrCategoryOrContent();

            String method = "";

            for (Object property : properties) {
                if (property instanceof JAXBElement) {
                    JAXBElement jaxbElement = (JAXBElement) property;

                    if (jaxbElement.getDeclaredType().isAssignableFrom(TextType.class)){//text types
                        TextType text = (TextType) jaxbElement.getValue();
                        String name = jaxbElement.getName().getLocalPart();
                        if (name.equals("title")){
                            method = text.getContent().toString().replaceAll("[\\[\\]]","");
                        }
                    }
                }
            }

            switch (method) {
                case "ingest" :
                    parseObjectCreated(entry);
                    break;
                case "modifyObject" :
                    parseModifyObject(entry);
                    break;
                case "purgeObject" :
                    parseObjectDeleted(entry);
                    break;
                case "addDatastream" :
                case "modifyDatastreamByReference" :
                case "modifyDatastreamByValue" :
                case "purgeDatastream" :
                case "setDatastreamState" :
                case "setDatastreamVersionable" :
                    parseDatastreamChanged(entry);
                    break;
                case "addRelationship" :
                case "purgeRelationship" :
                    parseRelationshipsChanged(entry);
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

    private void parseObjectCreated(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();

        String pid = null;
        Date date = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;

                if (jaxbElement.getDeclaredType().isAssignableFrom(ContentType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("content")) {
                        ContentType content = (ContentType) jaxbElement.getValue();
                        pid = content.getContent().get(0).toString();
                    }
                }

                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

            }
        }
        store.objectCreated(pid, date);

    }

    private void parseObjectDeleted(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();

        String pid = null;
        Date date = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;

                if (jaxbElement.getDeclaredType().isAssignableFrom(ContentType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("content")) {
                        ContentType content = (ContentType) jaxbElement.getValue();
                        pid = content.getContent().get(0).toString();
                    }
                }

                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

            }
        }
        store.objectDeleted(pid, date);

    }

    private void parseDatastreamChanged(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();



        String pid = null;
        String dsid = null;
        Date date = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;


                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

                if (jaxbElement.getDeclaredType().isAssignableFrom(CategoryType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("category")) {
                        CategoryType  category = (CategoryType) jaxbElement.getValue();
                        if (category.getScheme().equals("fedora-types:dsID")){
                            dsid = category.getTerm();
                        } else if (category.getScheme().equals("fedora-types:pid")){
                            pid = category.getTerm();
                        }
                    }
                }
            }
        }
        if (pid != null && date != null && dsid!= null){
            if (dsid.equals("RELS-EXT") || dsid.equals("RELS-INT")){
                store.objectRelationsChanged(pid,date);
            } else {
                store.objectChanged(pid,date);
            }
        }


    }

    private void parseRelationshipsChanged(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();



        String pid = null;
        String dsid = null;
        Date date = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;


                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

                if (jaxbElement.getDeclaredType().isAssignableFrom(CategoryType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("category")) {
                        CategoryType  category = (CategoryType) jaxbElement.getValue();
                        if (category.getScheme().equals("fedora-types:pid")){
                            pid = category.getTerm();
                        }
                    }
                }
            }
        }
        store.objectRelationsChanged(pid,date);
    }

    private void parseModifyObject(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();

        String pid = null;
        Date date = null;
        String newstate = null;
        for (Object property : properties) {
            if (property instanceof JAXBElement) {
                JAXBElement jaxbElement = (JAXBElement) property;


                if (jaxbElement.getDeclaredType().isAssignableFrom(DateTimeType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("updated")) {
                        DateTimeType datetime = (DateTimeType) jaxbElement.getValue();
                        date = datetime.getValue().toGregorianCalendar().getTime();
                    }
                }

                if (jaxbElement.getDeclaredType().isAssignableFrom(CategoryType.class)) {
                    if (jaxbElement.getName().getLocalPart().equals("category")) {
                        CategoryType  category = (CategoryType) jaxbElement.getValue();
                        if (category.getScheme().equals("fedora-types:state")){
                            newstate = category.getTerm();
                        } else if (category.getScheme().equals("fedora-types:pid")){
                            pid = category.getTerm();
                        }
                    }
                }
            }
        }
        if (pid != null && date != null && newstate != null){
            switch (newstate) {
                case "A": //published
                    store.objectPublished(pid, date);
                    break;
                case "I": //inProgress
                    store.objectChanged(pid, date);
                    break;
                case "D": //deleted
                    store.objectDeleted(pid, date);
                    break;
            }
        }


    }
}

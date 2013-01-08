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
import java.util.Collections;
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
        TextMessage msg = null;
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

            //TODO handle other methods like modifyRelations and the like
            if ("modifyObject".equals(method)){
                parseModifyObject(entry);
            } else if ("modifyDatastreamByValue".equals(method)
                       || "modifyDatastreamByReference".equals(method)
                       || "addDatastream".equals(method)){
                parseDatastreamChanged(entry);
            } else if ("ingest".equals(method)){
                parseObjectCreated(entry);
            } else{
                parseObjectChanged(entry);
            }



        } catch (JMSException e) {
            //TODO log this
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UpdateTrackerStorageException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (FedoraFailedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    private void parseObjectChanged(EntryType entry) throws UpdateTrackerStorageException, FedoraFailedException {
        List<Object> properties = entry.getAuthorOrCategoryOrContent();

        String pid = null;
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
        if (pid != null && date != null){
            store.objectChanged(pid,date);
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
                        pid = content.getContent().toString();
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
            if (newstate.equals("A")){//published
                store.objectPublished(pid,date);
            } else if (newstate.equals("I")){//inProgress
                store.objectChanged(pid,date);
            } else if (newstate.equals("D")){//deleted
                store.objectDeleted(pid,date);
            }
        }


    }
}

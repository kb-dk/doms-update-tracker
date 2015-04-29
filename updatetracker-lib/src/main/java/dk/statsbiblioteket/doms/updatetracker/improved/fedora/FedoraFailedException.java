package dk.statsbiblioteket.doms.updatetracker.improved.fedora;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/7/13
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class FedoraFailedException extends Exception{
    public FedoraFailedException(String message) {
        super(message);
    }

    public FedoraFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FedoraFailedException(Throwable cause) {
        super(cause);
    }

    public FedoraFailedException() {
    }
}

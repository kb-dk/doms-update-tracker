package dk.statsbiblioteket.doms.updatetracker.improved;

import dk.statsbiblioteket.doms.updatetracker.improved.fedora.FedoraFailedException;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

public class Utils {

    public static <T> T getCompleted(String contentModel, Set<Future<T>> results,
                                      ExecutorCompletionService<T> progressTracker) throws
                                                                                    FedoraFailedException {
        T changedRecords;
        try {
            Future<T> future = progressTracker.take();
            changedRecords = future.get();
        } catch (ExecutionException e) {
            cancelPendingTasks(results);
            throw new FedoraFailedException("Failed to calculate the changes caused by a change to content model '" +
                                            contentModel + "'", e);
        } catch (InterruptedException e) {
            cancelPendingTasks(results);
            throw new RuntimeException("Interrupted while waiting for results for recalculation of objects of '" +
                                       contentModel + "'", e);
        }
        return changedRecords;
    }


    public static <T> void cancelPendingTasks(Set<Future<T>> results) {
        for (Future<T> result : results) {
            result.cancel(true);
        }
    }
}

package hello1.koddata.concurrent;

import hello1.koddata.engine.QueryExecution;

public class KTask extends Thread {

    private final QueryExecution execution;

    public KTask(QueryExecution execution) {
        this.execution = execution;
    }

    @Override
    public void run() {
        super.run();
    }

    public QueryExecution getExecution() {
        return execution;
    }

    public void cancel() {
        // TODO: implement task cancellation
    }

    public boolean isCancelled() {
        // TODO: return cancellation status
        return false;
    }

    public void awaitCompletion() throws InterruptedException {
        // TODO: wait for task to complete
    }
}

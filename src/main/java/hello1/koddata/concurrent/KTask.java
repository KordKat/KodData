package hello1.koddata.concurrent;

import hello1.koddata.engine.QueryExecution;

public class KTask extends Thread {

    private final QueryExecution execution;
    private boolean isCancelled = false;

    public KTask(QueryExecution execution) {
        this.execution = execution;
    }

    @Override
    public void run() {

    }

    public QueryExecution getExecution() {
        return execution;
    }

    public void cancel() {
        this.interrupt();
        isCancelled = true;
    }

    public boolean isCancelled() {

        return isCancelled;
    }
}

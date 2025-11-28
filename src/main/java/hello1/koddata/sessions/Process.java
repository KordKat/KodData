package hello1.koddata.sessions;

import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.KTask;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.Value;

import java.util.concurrent.CompletableFuture;

public class Process {
    private static IdCounter idCounter = new IdCounter();
    private long processId;
    private KTask task;
    private CompletableFuture<Value<?>> completableFuture;
    public Process(KTask task){
        this.processId = idCounter.next();
        this.task = task;
    }

    public long id(){
        return processId;
    }

    public CompletableFuture<Value<?>> execute(){
        completableFuture = CompletableFuture.supplyAsync(task);
        return completableFuture;
    }

    public void interrupt(){
        if(completableFuture != null) {
            completableFuture.cancel(true);
        }
    }
}

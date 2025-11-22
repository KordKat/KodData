package hello1.koddata.sessions;

import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.KTask;
import hello1.koddata.concurrent.cluster.ConsistentCriteria;
import hello1.koddata.concurrent.cluster.Replica;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.Serializable;

import java.util.function.Consumer;

public class Process implements Replica {
    private static IdCounter idCounter = new IdCounter();
    private long processId;
    private long workerId;
    private KTask task;
    private Consumer<Value<?>> onComplete;
    private Consumer<Throwable> exceptionally;

    public Process(KTask task){
        onComplete = (e) -> {};
        exceptionally = Throwable::printStackTrace;
        this.processId = idCounter.next();
        this.task = task;
    }

    public void onComplete(Consumer<Value<?>> onComplete){
        this.onComplete = onComplete;
    }

    public void exceptionally(Consumer<Throwable> exceptionally){
        this.exceptionally = exceptionally;
    }

    public long id(){
        return processId;
    }

    public long workerId(){
        return workerId;
    }

    public void execute(){
        //TODO: worker pool execute KTask
    }

    public void interrupt(){

    }

    @Override
    public ConsistentCriteria getConsistencyCriteria() {
        return null;
    }

    @Override
    public void update(ConsistentCriteria latest) {

    }
}

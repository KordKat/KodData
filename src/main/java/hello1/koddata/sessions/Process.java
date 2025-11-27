package hello1.koddata.sessions;

import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.KTask;

public class Process {
    private static IdCounter idCounter = new IdCounter();
    private long processId;
    private KTask task;

    public Process(KTask task){
        this.processId = idCounter.next();
        this.task = task;
    }

    public long id(){
        return processId;
    }

    public void execute(){
        task.start();
    }

    public void interrupt(){

    }
}

package hello1.koddata.sessions;

import hello1.koddata.concurrent.IdCounter;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Session {

    private static IdCounter idCounter = new IdCounter();

    private long sessionId;
    private final long startedTime;
    private long lastActive;
    private SessionSettings settings;
    private HashMap<Long, Process> processes;
    private SessionData sessionData;
    public enum State {
        RUNNING(0),
        IDLE(1),
        SUSPENDED(2),
        TERMINATED(3);

        State(int ord){
            assert ordinal() == ord;
        }
    }

    private State state;

    private Session(SessionSettings settings){
        sessionId = idCounter.next();
        startedTime = System.currentTimeMillis();
        lastActive = System.currentTimeMillis();
        this.state = State.IDLE;
        this.settings = settings;
    }



    public static Session newSession(long userId){
        return null;
    }

    public boolean isProcessPresent(long processId){
        return processes.containsKey(processId);
    }

    public long id(){
        return sessionId;
    }

    public long startedTime(){
        return startedTime;
    }

    public long lastActive(){
        return lastActive;
    }

    public void state(State state){
        this.state = state;
    }

    public State state(){
        return state;
    }

    public void listRunningProcesses(){}
    public void cancelProcess(long processId){}

    public long run(byte[] b){
        return -1;
    }

    public void terminate(){}


}

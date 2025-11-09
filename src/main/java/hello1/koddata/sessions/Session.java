package hello1.koddata.sessions;

import hello1.koddata.concurrent.IdCounter;

import java.nio.channels.SocketChannel;
import java.util.Set;

public class Session {

    private static IdCounter idCounter = new IdCounter();

    private long sessionId;
    private final long startedTime;
    private long lastActive;
    private SessionSettings settings;
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

    public long newSession(String userId){
        return 0;
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

}

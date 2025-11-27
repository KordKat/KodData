package hello1.koddata.sessions;

import hello1.koddata.Main;
import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.KTask;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.QueryExecution;
import hello1.koddata.sessions.users.User;
import java.util.HashMap;
import java.util.List;

public class Session {

    private static final IdCounter idCounter =
            new IdCounter();

    private long sessionId;
    private User user;
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
    private Session(SessionSettings settings , User user){
        sessionId = idCounter.next();
        startedTime = System.currentTimeMillis();
        lastActive = System.currentTimeMillis();
        this.state = State.IDLE;
        this.settings = settings;
        this.user = user;
    }


    public static Session newSession(User user){
        SessionSettings sessionSettings = new SessionSettings(user.getUserData().userPrivilege().maxProcessPerSession(), user.getUserData().userPrivilege().maxMemoryPerProcess());
        Session session = new Session(sessionSettings , user);
        Main.bootstrap.getSessionManager().putSession(session.sessionId, session);
        return session;
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

    public List<Process> listRunningProcesses(){
        return processes.values().stream().toList();
    }
    public void cancelProcess(long processId){
        if(processes.containsKey(processId)){
            processes.get(processId).interrupt();
            processes.remove(processId);
        }
    }

    public long newProcess(QueryExecution execution, ColumnArray columnArray){
        Process process = new Process(new KTask(execution, columnArray));
        this.processes.put(process.id(), process);
        process.execute();
        return process.id();
    }

    public void terminate(){
        for(Process process : processes.values()){
            process.interrupt();
        }

        processes.clear();
        state = State.TERMINATED;
    }

    public SessionSettings getSettings() {
        return settings;
    }

    public SessionData getSessionData() {
        return sessionData;
    }

    public User getUser() {
        return user;
    }
}

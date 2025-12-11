package hello1.koddata.sessions;

import hello1.koddata.Main;
import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.KTask;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.engine.QueryExecution;
import hello1.koddata.engine.Value;
import hello1.koddata.net.UserClient;
import hello1.koddata.sessions.users.User;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Session {

    private static final IdCounter idCounter =
            new IdCounter();

    //Encapsulation
    private long sessionId;
    //Encapsulation
    private SessionSettings settings;
    private HashMap<Long, Process> processes;
    //Encapsulation
    private SessionData sessionData;
    //Encapsulation
    private UserClient userClient;

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
    private Session(SessionSettings settings , UserClient userClient){
        sessionId = idCounter.next();
        this.state = State.IDLE;
        this.settings = settings;
        this.userClient = userClient;
        this.sessionData = new SessionData();
        this.processes = new HashMap<>();
    }


    public static Session newSession(UserClient userClient){
        SessionSettings sessionSettings = new SessionSettings(userClient.getUser().getUserData().userPrivilege().maxProcessPerSession(), userClient.getUser().getUserData().userPrivilege().maxMemoryPerProcess());
        Session session = new Session(sessionSettings , userClient);
        Main.bootstrap.getSessionManager().putSession(session.sessionId, session);
        return session;
    }

    public boolean isProcessPresent(long processId){
        return processes.containsKey(processId);
    }

    //Encapsulation
    public long id(){
        return sessionId;
    }

    //Encapsulation
    public void state(State state){
        this.state = state;
    }

    //Encapsulation
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

    public CompletableFuture<Value<?>> newProcess(QueryExecution execution, ColumnArray columnArray){
        Process process = new Process(new KTask(execution, columnArray, this));
        this.processes.put(process.id(), process);
        return process.execute();
    }

    public void terminate(){
        for(Process process : processes.values()){
            process.interrupt();
        }

        processes.clear();
        state = State.TERMINATED;
    }

    //Encapsulation
    public SessionSettings getSettings() {
        return settings;
    }

    //Encapsulation
    public SessionData getSessionData() {
        return sessionData;
    }

    //Encapsulation
    public UserClient getUser() {
        return userClient;
    }
}

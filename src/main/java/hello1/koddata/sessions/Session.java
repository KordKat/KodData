package hello1.koddata.sessions;

import hello1.koddata.Main;
import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.engine.StatementExecutor;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.Lexer;
import hello1.koddata.kodlang.Parser;
import hello1.koddata.kodlang.Token;
import hello1.koddata.kodlang.ast.SemanticAnalyzer;
import hello1.koddata.kodlang.ast.Statement;
import hello1.koddata.net.NodeStatus;
import hello1.koddata.sessions.users.User;
import hello1.koddata.utils.collection.ImmutableArray;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class Session {

    private static final ClusterIdCounter idCounter =
            ClusterIdCounter.getCounter(Session.class,
                    new HashSet<>(Main.bootstrap.getServer().getStatusMap().values()));

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

    private Session(SessionSettings settings ,User user){
        sessionId = idCounter.count();
        startedTime = System.currentTimeMillis();
        lastActive = System.currentTimeMillis();
        this.state = State.IDLE;
        this.settings = settings;
        this.user = user;
    }

    public long executeCode(String code, SocketChannel socketChannel) throws KException {
        Token[] tokens = Lexer.analyze(code.toCharArray());
        Parser parser = new Parser(new ImmutableArray<>(tokens));
        Statement statement = parser.parseStatement();
        new SemanticAnalyzer().analyze(statement);
        return StatementExecutor.executeStatement(statement, this, socketChannel);
    }

    public static Session newSession(User user){
        SessionSettings sessionSettings = new SessionSettings(user.getUserData().userPrivilege().maxProcessPerSession(), user.getUserData().userPrivilege().maxMemoryPerProcess());
        return new Session(sessionSettings , user);
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

    public long run(byte[] b){
        return -1;
    }

    public void terminate(){
        for(Process process : processes.values()){
            process.interrupt();
        }

        processes.clear();
        state = State.TERMINATED;

    }



    public SessionData getSessionData() {
        return sessionData;
    }
}

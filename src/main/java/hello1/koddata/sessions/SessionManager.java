package hello1.koddata.sessions;

import hello1.koddata.utils.ref.EmptyCleaner;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SessionManager {

    private ConcurrentMap<Long, Session> sessions = new ConcurrentHashMap<>();

    public void terminateSession(long sessionId){
        Session session = sessions.get(sessionId);
        if(session == null) return;

        session.terminate();
        sessions.remove(sessionId);

    }

    public void terminateAllSession(){
        for (long l : sessions.keySet()){
            terminateSession(l);
        }
    }

    public  Session.State sessionStatus(long sessionId){
        return sessions.get(sessionId) == null ? null : sessions.get(sessionId).state();
    }

    public Session getSession(long sessionId){
        return sessions.get(sessionId);
    }

    public SessionSettings settingSession(long sessionId){
        return sessions.get(sessionId).getSettings();
    }

    public List<Session> sessionList(){
        return sessions.values().stream().toList();
    }

    public List<Session> listActiveSessions(){
        return sessions.values().stream().filter(x -> x.state() == Session.State.IDLE ||  x.state() == Session.State.RUNNING).toList();
    }

    void putSession(long id, Session session){
        sessions.put(id, session);
    }
}

package hello1.koddata.sessions;

import hello1.koddata.net.UserClient;
import hello1.koddata.utils.ref.EmptyCleaner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SessionManager {

    private ConcurrentMap<Long, Session> sessions = new ConcurrentHashMap<>();

    public void terminateSession(long sessionId){
        Session session = sessions.get(sessionId);
        if(session == null) return;
        UserClient userClient = session.getUser();
        try {
            userClient.getSocketChannel().close();

        } catch (IOException e) {

        }
        session.terminate();
        sessions.remove(sessionId);

    }

    public void terminateAllSession(){
        for (long l : sessions.keySet()){
            terminateSession(l);
        }
    }


    public Session getSession(long sessionId){
        return sessions.get(sessionId);
    }

    public List<Session> sessionList(){
        return sessions.values().stream().toList();
    }


    void putSession(long id, Session session){
        sessions.put(id, session);
    }
}

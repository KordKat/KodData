package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.concurrent.cluster.ConsistentCriteria;
import hello1.koddata.concurrent.cluster.Replica;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class User implements Replica {

    private List<Session> userSession;
    private Session currentlySession;
    private UserData userData;
    User(UserData userData) {
        userSession = new CopyOnWriteArrayList<>();
        currentlySession = null;
        this.userData = userData;
    }

    public Session newSession() throws KException {
        if(userData.userPrivilege().maxSession() <= userSession.size()) throw new KException(ExceptionCode.KDR0009, "Cannot create more session for this user");
        Session session = Session.newSession(this);
        userSession.add(session);
        return session;
    }
    public List<Session> listSessions(){
        return userSession;
    }
    public Session currentlySession(){
        return currentlySession;
    }
    public void logOut(){
        Main.bootstrap.getUserManager().logoutUser(userData.userId());
    }
    public void terminateTask(long processId){
        for (Session session : userSession){
            if(session.isProcessPresent(processId)) {
                session.cancelProcess(processId);
                return;
            }
        }
    }

    public UserData getUserData() {
        return userData;
    }
    public void setUserData(UserData userData) {
        this.userData = userData;
    }

    @Override
    public ConsistentCriteria getConsistencyCriteria() {
        return null;
    }

    @Override
    public void update(ConsistentCriteria latest) {

    }
}

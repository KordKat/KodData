package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.concurrent.cluster.ConsistentCriteria;
import hello1.koddata.concurrent.cluster.Replica;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class User implements Replica {

    private List<Session> userSession;
    private UserData userData;
    private ReplicatedResourceClusterReference<User> ref;

    User(UserData userData) {
        userSession = new CopyOnWriteArrayList<>();
        this.userData = userData;
    }

    public void setRef(ReplicatedResourceClusterReference<User> ref) {
        this.ref = ref;
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
        ConsistentCriteria criteria = new ConsistentCriteria();
        criteria.setLatestUpdate(ref.getLatestUpdate());
        criteria.addCriteria("name", userData.name());
        criteria.addCriteria("password", userData.password());
        criteria.addCriteria("isAdmin", userData.isAdmin());
        criteria.addCriteria("priv.maxSession", userData.userPrivilege().maxSession());
        criteria.addCriteria("priv.maxProcessPerSession", userData.userPrivilege().maxProcessPerSession());
        criteria.addCriteria("priv.maxMemoryPerProcess", userData.userPrivilege().maxMemoryPerProcess());
        criteria.addCriteria("priv.maxStorageUsage", userData.userPrivilege().maxStorageUsage());

        return criteria;
    }

    @Override
    public void update(ConsistentCriteria latest) {
        if(latest.isNewerThan(getConsistencyCriteria())){
            String newName = (String) latest.get("name");
            String newPass = (String) latest.get("password");
            int maxSession = (int) latest.get("priv.maxSession");
            int maxProcessPerSession = (int) latest.get("priv.maxProcessPerSession");
            int maxMemoryPerProcess = (int) latest.get("priv.maxMemoryPerProcess");
            int maxStorageUsage = (int) latest.get("priv.maxStorageUsage");
            boolean newIsAdmin = (boolean) latest.get("isAdmin");
            UserPrivilege newPrivilege = new UserPrivilege(maxSession, maxProcessPerSession, maxMemoryPerProcess, maxStorageUsage);
            this.userData = new UserData(userData.userId(), newName, newPrivilege, newPass, newIsAdmin);
        }
    }
}

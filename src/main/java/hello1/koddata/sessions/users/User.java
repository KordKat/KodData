package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class User {

    private List<Session> userSession;
    private long currentlySession;
    private UserData userData;

    User(UserData userData) {
        userSession = new CopyOnWriteArrayList<>();
        currentlySession = -1;
        this.userData = userData;
    }

    public Session newSession(){return null;}
    public List<Session> listSessions(){return null;}
    public long currentlySession(){return -1;}
    public void logOut(){}
    public void terminateTask(long processId){}
    public byte[] userToByteArrays(){return null;}

    public UserData getUserData() {
        return userData;
    }


}

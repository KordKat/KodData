package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;

import java.util.ArrayList;
import java.util.List;

public class User {

    List<Session> userSession;
    long currentlySession;
    UserData userData;


    public static User logIn(long userId ,long password){return null;}

    public Session newSession(){return null;}
    public List<Session> listMySessions(){return null;}
    public long currentlySession(){return -1;}
    public void logOut(){}
    public void terminateTask(long processId){}
    public byte[] userToByteArrays(){return null;}
}

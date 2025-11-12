package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;

import java.util.ArrayList;
import java.util.List;

public class User {
    long userId;
    String name;
    List<Session> userSession;
    long currentlySession;


    public static User logIn(long userId ,long password){return null;}
    public void logOut(){}
    public Session newSession(){return null;}
    public long currentlySession(){return -1;}
    public void terminateTask(long processId){}


    public void listMySessions(){}
    public void sessionSettings(){}
}

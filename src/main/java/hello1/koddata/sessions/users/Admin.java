package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;

public class Admin extends User {

    public static User editUser(long userId){return null;}
    public void changeUserQueue(long userId){}
    public void deleteUser(long userId){}
    public void createUser(){}
    public void userList(){}
    public void  changeUserPassword(long userId){}

    public void endSession(long sessionId){}
    public void endAllSession(){}
    public static Session.State sessionStatus(long sessionId){return null;}
    public void sessionList(){}
    public static Session settingSession(){return null;}



    public void updateUser(long userId){}
    public void deactivateUser(long userId){}
    public void reactivateUser(long userId){}
    public void listActiveSessions(){}
    public void listUserSessions(long userId){}
    public void inspectSession(long sessionId){}

//    public void setMaxSessionProcess(long sessionId, int n){}

    public void listRunningProcesses(long sessionId){}
    public void cancelProcess(long sessionId, long processId){}
//    public void changeQueue(long userId, long toQueueId){}
}

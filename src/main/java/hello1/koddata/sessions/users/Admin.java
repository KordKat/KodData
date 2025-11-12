package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionManager;

public class Admin extends User {

    UserManager userManager;
    SessionManager sessionManager;

    public UserManager getUserManager() {
        return userManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }
}

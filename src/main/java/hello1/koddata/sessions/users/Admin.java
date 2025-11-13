package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.SessionManager;

public class Admin extends User {

    private UserManager userManager;
    private SessionManager sessionManager;

    Admin(UserData userData, UserManager userManager, SessionManager sessionManager) {
        super(userData);
        this.userManager = userManager;
        this.sessionManager = sessionManager;
    }

    public UserManager getUserManager() {
        return userManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }
}

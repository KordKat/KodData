package hello1.koddata.engine;

import hello1.koddata.net.Server;
import hello1.koddata.net.util.NodeConnectionData;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.utils.SerialVersionId;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

public class Bootstrap {

    private long serialVersionId = SerialVersionId.get;
    private File nodeCfgFile;
    private Server server;
    private Path rootPath;
    private UserManager userManager;
    private SessionManager sessionManager;

    public void start(String[] args) {

    }

    public void end(){}

    private void startServer() {}


    public Server getServer() {
        return server;
    }

    public Path getRootPath() {
        return rootPath;
    }

    public File getNodeCfgFile() {
        return nodeCfgFile;
    }

    public UserManager getUserManager() {
        return userManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }
}

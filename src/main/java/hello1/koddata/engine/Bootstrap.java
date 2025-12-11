package hello1.koddata.engine;

import hello1.koddata.exception.KException;
import hello1.koddata.net.*;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.sessions.users.UserData;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.sessions.users.UserPrivilege;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

public class Bootstrap {
    private Path rootPath;
    private UserManager userManager;
    private SessionManager sessionManager;
    private UserServiceServer userServiceServer;
    private SocketFactory userServiceServerFactory;

    private static final Properties config = new Properties();

    public void start(String[] args) throws IOException, KException, InterruptedException {
        configure(args);
        startServer();
    }

    public void end() throws IOException, InterruptedException {
        userServiceServer.stop();
        Thread.sleep(5000);
        userManager.saveUserData();
        System.exit(0);
    }

    private void configure(String[] args) throws IOException, KException, InterruptedException {
        assert args.length >= 1;

        String cfgFile = args[0];
        config.load(new FileInputStream(cfgFile));
        rootPath = Path.of(config.getProperty("root", "koddata/"));
        userManager = new UserManager(this);
        userManager.loadAllUserData();
        Thread.sleep(1000);
        if(userManager.getUserDataMap().isEmpty()){
            userManager.createUser(new UserData(0, "root", new UserPrivilege(2, 2, 2, 2), "1234", true));
            userManager.saveUserData();
        }

        sessionManager = new SessionManager();

        int userBufferSize = Integer.parseInt(config.getProperty("server.user.bufferSize"));
        boolean userTcpNoDelay = Boolean.parseBoolean(config.getProperty("server.user.tcoNoDelay"));
        boolean userReuseAddr = Boolean.parseBoolean(config.getProperty("server.user.reuseAddr"));
        boolean userKeepAlive = Boolean.parseBoolean(config.getProperty("server.user.keepAlive"));

        userServiceServerFactory = new SocketFactory.Builder()
                .readBufferSize(userBufferSize)
                .sendBufferSize(userBufferSize)
                .tcpNoDelay(userTcpNoDelay)
                .reuseAddress(userReuseAddr)
                .keepAlive(userKeepAlive)
                .get();

    }

    private void startServer() {
        try {
            startUserServiceServer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startUserServiceServer() throws IOException {
        if(userServiceServer == null || !userServiceServer.running){
            userServiceServer = new UserServiceServer(new InetSocketAddress(Integer.parseInt(config.getProperty("server.user.port"))), userServiceServerFactory);
            userServiceServer.start();
        }
    }

//       Encapsulation

    public Path getRootPath() {
        return rootPath;
    }
//       Encapsulation

    public UserManager getUserManager() {
        return userManager;
    }
//       Encapsulation

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public static Properties getConfig() {
        return config;
    }
}

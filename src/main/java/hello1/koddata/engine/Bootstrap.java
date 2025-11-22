package hello1.koddata.engine;

import hello1.koddata.exception.KException;
import hello1.koddata.net.*;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.utils.SerialVersionId;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class Bootstrap {

    private long serialVersionId = SerialVersionId.get;
    private File nodeCfgFile;
    private GossipServer gossipServer;
    private Path rootPath;
    private UserManager userManager;
    private SessionManager sessionManager;
    private DataTransferServer dataTransferServer;
    private UserServiceServer userServiceServer;
    private SocketFactory gossipServerFactory;
    private SocketFactory dataTransferSocketFactory;
    private SocketFactory userServiceServerFactory;

    private static final Properties config = new Properties();

    public void start(String[] args) {

    }

    public void end(){

    }

    private void startServer() {
        try {
            startGossipServer();
            startDataTransfer();
            startUserServiceServer();
        } catch (KException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startDataTransfer() throws IOException {
        if(dataTransferServer == null || !dataTransferServer.running){
            dataTransferServer = new DataTransferServer(new InetSocketAddress(Integer.parseInt(config.getProperty("server.data.port"))), dataTransferSocketFactory);
            dataTransferServer.start();
        }
    }

    private void startGossipServer() throws KException {
        if(gossipServer == null || !gossipServer.running){
            gossipServer = new GossipServer(new InetSocketAddress(Integer.parseInt(config.getProperty("server.gossip.port"))), gossipServerFactory);
            gossipServer.start();
        }
    }

    private void startUserServiceServer() throws IOException {
        if(userServiceServer == null || !userServiceServer.running){
            userServiceServer = new UserServiceServer(new InetSocketAddress(Integer.parseInt(config.getProperty("server.user.port"))), userServiceServerFactory);
            userServiceServer.start();
        }
    }


    public GossipServer getServer() {
        return gossipServer;
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

    public static Properties getConfig() {
        return config;
    }
}

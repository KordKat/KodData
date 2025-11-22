package hello1.koddata.engine;

import hello1.koddata.exception.KException;
import hello1.koddata.net.*;
import hello1.koddata.sessions.SessionManager;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.utils.SerialVersionId;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
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

    public void start(String[] args) throws IOException {
        configure(args);
        startServer();
    }

    public void end() throws IOException, InterruptedException {
        dataTransferServer.stop();
        userServiceServer.stop();
        gossipServer.stop();
        Thread.sleep(5000); //ensure every is closed properly
        userManager.saveUserData();
    }

    private void configure(String[] args)throws IOException{
        assert args.length >= 1;

        String cfgFile = args[0];
        config.load(new FileInputStream(cfgFile));
        String nodeFileName = config.getProperty("nodefile", "nodes.txt");
        nodeCfgFile = new File(nodeFileName);
        rootPath = Path.of(URI.create(config.getProperty("root", "koddata/")));
        userManager = new UserManager(this);
        sessionManager = new SessionManager();

        int dataBufferSize = Integer.parseInt(config.getProperty("server.data.bufferSize"));
        boolean dataTcpNoDelay = Boolean.parseBoolean(config.getProperty("server.data.tcoNoDelay"));
        boolean dataReuseAddr = Boolean.parseBoolean(config.getProperty("server.data.reuseAddr"));
        boolean dataKeepAlive = Boolean.parseBoolean(config.getProperty("server.data.keepAlive"));

        dataTransferSocketFactory = new SocketFactory.Builder()
                .readBufferSize(dataBufferSize)
                .sendBufferSize(dataBufferSize)
                .tcpNoDelay(dataTcpNoDelay)
                .reuseAddress(dataReuseAddr)
                .keepAlive(dataKeepAlive)
                .get();

        int gossipBufferSize = Integer.parseInt(config.getProperty("server.gossip.bufferSize"));
        boolean gossipTcpNoDelay = Boolean.parseBoolean(config.getProperty("server.gossip.tcoNoDelay"));
        boolean gossipReuseAddr = Boolean.parseBoolean(config.getProperty("server.gossip.reuseAddr"));
        boolean gossipKeepAlive = Boolean.parseBoolean(config.getProperty("server.gossip.keepAlive"));

        gossipServerFactory = new SocketFactory.Builder()
                .readBufferSize(gossipBufferSize)
                .sendBufferSize(gossipBufferSize)
                .tcpNoDelay(gossipTcpNoDelay)
                .reuseAddress(gossipReuseAddr)
                .keepAlive(gossipKeepAlive)
                .get();

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

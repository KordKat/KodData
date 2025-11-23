package hello1.koddata.client;

import java.io.IOException;

public class ServiceBootstrap {

    private ServerService serverService;
    private TerminalService terminalService;

    public void start(String[] args) throws IOException {
        startTerminalService();
    }

    public void startServerService(){
        serverService = new ServerService("", 0, this);
    }

    public void startTerminalService() throws IOException {
        terminalService = new TerminalService(this);
    }

    public ServerService getServerService() {
        return serverService;
    }

    public TerminalService getTerminalService() {
        return terminalService;
    }
}

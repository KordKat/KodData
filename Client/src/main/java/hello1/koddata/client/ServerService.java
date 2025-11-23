package hello1.koddata.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ServerService {

    private final String clusterHost;
    private final int clusterPort;

    private final Queue<String> messageQueue = new LinkedBlockingQueue<>();
    private Socket socket;
    private PrintWriter printer;
    private Thread readThread;
    private Thread writeThread;
    private volatile boolean running = false;
    private ServiceBootstrap bootstrap;
    public ServerService(String clusterHost, int clusterPort, ServiceBootstrap bootstrap){
        this.clusterPort = clusterPort;
        this.clusterHost = clusterHost;
        this.bootstrap = bootstrap;
    }

    public void start() throws IOException {
        if (running) return;
        running = true;

        socket = new Socket(clusterHost, clusterPort);
        printer = new PrintWriter(socket.getOutputStream(), true);

        readThread = new Thread(this::readLoop, "ServerService-Read");
        writeThread = new Thread(this::writeLoop, "ServerService-Write");

        readThread.start();
        writeThread.start();
    }

    public void enqueueMessage(String msg) {
        if (!running) throw new IllegalStateException("Service is not running");
        messageQueue.add(msg);
    }

    private void readLoop() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {

            String line;
            while (running && (line = reader.readLine()) != null) {
                handleIncomingMessage(line);
            }

        } catch (IOException e) {
            if (running) {
                handleIncomingMessage("Read loop error: " + e.getMessage());
            }
        } finally {
            stop();
        }
    }

    private void handleIncomingMessage(String msg) {
        bootstrap.getTerminalService().enqueueMessage(msg);
    }

    private void writeLoop() {
        try {
            while (running) {
                String msg = messageQueue.poll();
                if (msg != null) {
                    printer.println(msg);
                }
                Thread.yield();
            }
        }finally {
            stop();
        }
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;

        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {}

        if (readThread != null && readThread.isAlive()) readThread.interrupt();
        if (writeThread != null && writeThread.isAlive()) writeThread.interrupt();
    }
}

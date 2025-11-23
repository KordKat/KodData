package hello1.koddata.client;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TerminalService {

    private Terminal terminal;

    private LineReader reader;

    private Queue<String> messageQueue = new LinkedBlockingQueue<>();

    private Thread printThread;
    private Thread readThread;

    private volatile boolean running;

    private ServiceBootstrap bootstrap;

    public TerminalService(ServiceBootstrap bootstrap) throws IOException {
        terminal = TerminalBuilder.builder().system(true).build();
        reader = LineReaderBuilder.builder().terminal(terminal).build();
        printThread = new Thread(this::runPrint);
        readThread = new Thread(this::runRead);
        this.bootstrap = bootstrap;
    }

    public void start(){
        printThread.start();
        readThread.start();
    }

    public void enqueueMessage(String message){
        messageQueue.offer(message);
    }

    public void runPrint(){
        running = true;
        while(running){
            while(messageQueue.isEmpty()){
                String s = messageQueue.poll();
                reader.printAbove(s);
            }
        }
    }

    public void runRead(){
        running = true;
        while(running){
            String read = reader.readLine("KodData> ");
            bootstrap.getServerService().enqueueMessage(read);
        }
    }

    public void stop(){
        running = false;
    }

}

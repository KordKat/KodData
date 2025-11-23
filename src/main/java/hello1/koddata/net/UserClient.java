package hello1.koddata.net;

import hello1.koddata.engine.StatementExecutor;
import hello1.koddata.exception.KException;
import hello1.koddata.kodlang.Lexer;
import hello1.koddata.kodlang.Parser;
import hello1.koddata.kodlang.Token;
import hello1.koddata.kodlang.ast.SemanticAnalyzer;
import hello1.koddata.kodlang.ast.Statement;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.users.User;
import hello1.koddata.utils.collection.ImmutableArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class UserClient {

    private User user;
    private Session currentSession;
    private final Selector selector;
    private final SocketChannel socketChannel;

    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

    public UserClient(Selector selector, SocketChannel sc){
        this.selector = selector;
        this.socketChannel = sc;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Session getCurrentSession() {
        return currentSession;
    }

    public void setCurrentSession(Session currentSession) {
        this.currentSession = currentSession;
    }

    public void write(ByteBuffer buffer){
        writeQueue.offer(buffer);
        SelectionKey key = socketChannel.keyFor(selector);
        if(key != null && key.isValid()){
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    public void processWrite() throws IOException {
        while(!writeQueue.isEmpty()){
            ByteBuffer buffer = writeQueue.peek();
            socketChannel.write(buffer);

            if(buffer.hasRemaining()){
                return;
            }

            writeQueue.poll();
        }

        SelectionKey key = socketChannel.keyFor(selector);
        if(key != null && key.isValid()){
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    public boolean isLoggedIn(){
        return user != null;
    }

    public void executeCode(String code) throws KException {
        Token[] tokens = Lexer.analyze(code.toCharArray());
        Parser parser = new Parser(new ImmutableArray<>(tokens));
        Statement statement = parser.parseStatement();
        new SemanticAnalyzer().analyze(statement);
        StatementExecutor.executeStatement(statement, this);
    }

}
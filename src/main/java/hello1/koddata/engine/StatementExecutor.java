package hello1.koddata.engine;

import hello1.koddata.kodlang.ast.AssignmentExpression;
import hello1.koddata.kodlang.ast.BlockStatement;
import hello1.koddata.kodlang.ast.Expression;
import hello1.koddata.kodlang.ast.Statement;
import hello1.koddata.net.UserClient;
import hello1.koddata.sessions.Session;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class StatementExecutor {

    public static void executeStatement(Statement statement, UserClient client){
        if(statement instanceof BlockStatement block){
            for(Statement stmt : block.statements){
                executeStatement(stmt, client);
            }
        }else if(statement instanceof Expression expression){
            Value<?> result = evaluateExpression(expression, client);
            client.write(ByteBuffer.wrap(("- " + result.get().getClass().getName()).getBytes(StandardCharsets.UTF_8)));
        }
    }

    private static Value<?> evaluateExpression(Expression expression, UserClient client){
        
        return new NullValue(new Object());
    }

}

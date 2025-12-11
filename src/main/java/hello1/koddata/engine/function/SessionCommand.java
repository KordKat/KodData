package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

import java.util.List;

public class SessionCommand extends KodFunction<Session>{
    @Override
    public Value<Session> execute() throws KException {
        if (!arguments.containsKey("command")){
            throw new KException(ExceptionCode.KDE0012,"You need to write command to process user command");
        }
        Value<?> command = arguments.get("command");
        if (command.get().equals("terminateSession")){
            if (!arguments.containsKey("sessionId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write userId to terminate session");
            }
            Value<?> userId = arguments.get("sessionId");
            if (!(userId.get() instanceof Number sessionIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "sessionId should be Long");
            }
            terminateSession(sessionIdLong.longValue());
        }
        else if (command.get().equals("terminateAllSession")){
            terminateAllSession();
        }
        else if (command.get().equals("sessionList")){
            sessionList();
        }
        return new Value<>(null);

    }

    public void terminateSession(Long sessionId){
        Main.bootstrap.getSessionManager().terminateSession(sessionId);
    }
    public void terminateAllSession(){
        Main.bootstrap.getSessionManager().terminateAllSession();
    }

    public List<Session> sessionList(){
        return Main.bootstrap.getSessionManager().sessionList();
    }
}

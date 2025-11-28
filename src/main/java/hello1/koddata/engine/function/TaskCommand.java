package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.concurrent.KTask;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Process;
import hello1.koddata.sessions.users.UserData;

import java.util.List;

public class TaskCommand extends KodFunction<KTask>{
    @Override
    public Value<KTask> execute() throws KException {
        if (!arguments.containsKey("command")){
            throw new KException(ExceptionCode.KDE0012,"You need to write command to process user command");
        }
        Value<?> command = arguments.get("command");
        if (command.get().equals("cancelTask")){
            if (!arguments.containsKey("taskId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write taskId to cancel task");
            }
            Value<?> taskId = arguments.get("taskId");
            if (!(taskId.get() instanceof Long taskIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "taskId should be Long");
            }
            if (!arguments.containsKey("userId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write taskId to cancel task");
            }
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "taskId should be Long");
            }
            cancelTask(userIdLong , taskIdLong);
        }
        else if (command.get().equals("taskList")){
            if (!arguments.containsKey("sessionId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write userId to terminate session");
            }
            Value<?> userId = arguments.get("sessionId");
            if (!(userId.get() instanceof Long sessionIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "sessionId should be Long");
            }
            List<Process> task = taskList(sessionIdLong);
        }
        return new Value<>(null);
    }

    public void cancelTask(long userId , long taskId) throws KException {
        UserData user = Main.bootstrap.getUserManager().findUser(userId).getUserData();
        if (user == null){
            throw new KException(ExceptionCode.KDE0012, "User does not exist");
        }
        Main.bootstrap.getUserManager().findUser(userId).terminateTask(taskId);

    }

    public List<Process> taskList(long sessionIdLong){
        return Main.bootstrap.getSessionManager().getSession(sessionIdLong).listRunningProcesses();
    }
}

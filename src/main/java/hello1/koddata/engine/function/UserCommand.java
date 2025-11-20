package hello1.koddata.engine.function;

import hello1.koddata.concurrent.KTask;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.sessions.users.User;
import hello1.koddata.sessions.users.UserData;
import hello1.koddata.sessions.users.UserPrivilege;

import java.util.List;

public class UserCommand extends KodFunction<User>{
    @Override
    public Value<User> execute() throws KException {
        if (!arguments.containsKey("command")){
            throw new KException(ExceptionCode.KDE0012,"You need to write command to process user command");
        }
        Value<?> command = arguments.get("command");
        if (!(command.get() instanceof String commandString)) {
            throw new KException(ExceptionCode.KDE0012, "command should be string");
        }
        if (command.get().equals("create")){
            if (!arguments.containsKey("name")){
                throw new KException(ExceptionCode.KDE0012,"You need to write name to create user");
            }
            Value<?> name = arguments.get("name");
            if (!(name.get() instanceof String nameString)) {
                throw new KException(ExceptionCode.KDE0012, "name should be string");
            }
            if (!arguments.containsKey("maximumResource")){
                throw new KException(ExceptionCode.KDE0012,"You need to write maximumResource to create user");
            }
            Value<?> maximumResource = arguments.get("maximumResource");
            if (!(maximumResource.get() instanceof UserPrivilege maximumResourceUP)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!arguments.containsKey("password")){
                throw new KException(ExceptionCode.KDE0012,"You need to write password to create user");
            }
            Value<?> password = arguments.get("password");
            if (!(password.get() instanceof String passwordString)) {
                throw new KException(ExceptionCode.KDE0012, "password should be String");
            }
            if (!arguments.containsKey("isAdmin")){
                throw new KException(ExceptionCode.KDE0012,"You need to write isAdmin to create user");
            }
            Value<?> isAdmin = arguments.get("isAdmin");
            if (!(password.get() instanceof Boolean isAdminB)) {
                throw new KException(ExceptionCode.KDE0012, "isAdmin should be Logical");
            }
            createUser(nameString , maximumResourceUP , passwordString , isAdminB );
        }
        else if (command.get().equals("edit")){
            if (!arguments.containsKey("userId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write userId to edit user");
            }
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "userId should be Long");
            }
            Value<?> name = arguments.get("name");
            if (!(name.get() instanceof String nameString)) {
                throw new KException(ExceptionCode.KDE0012, "name should be string");
            }
            Value<?> maximumResource = arguments.get("maximumResource");
            if (!(maximumResource.get() instanceof UserPrivilege maximumResourceUP)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            Value<?> password = arguments.get("password");
            if (!(password.get() instanceof String passwordString)) {
                throw new KException(ExceptionCode.KDE0012, "password should be String");
            }
            Value<?> isAdmin = arguments.get("isAdmin");
            if (!(password.get() instanceof Boolean isAdminB)) {
                throw new KException(ExceptionCode.KDE0012, "isAdmin should be Logical");
            }
            editUser(userIdLong , nameString , maximumResourceUP , passwordString , isAdminB );
        }
        else if (command.get().equals("remove")){
            if (!arguments.containsKey("userId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write userId to remove user");
            }
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "userId should be Long");
            }
            removeUser(userIdLong);

        }
        else if (command.get().equals("userlist")){
            userList();
        }
        else if (command.get().equals("terminateSession")){
            if (!arguments.containsKey("sessionId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write userId to terminate session");
            }
            Value<?> userId = arguments.get("sessionId");
            if (!(userId.get() instanceof Long sessionIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "sessionId should be Long");
            }
            terminateSession(sessionIdLong);
        }
        else if (command.get().equals("sessionList")){
            sessionList();
        }
        else if (command.get().equals("cancelTask")){
            if (!arguments.containsKey("taskId")){
                throw new KException(ExceptionCode.KDE0012,"You need to write taskId to cancel task");
            }
            Value<?> taskId = arguments.get("taskId");
            if (!(taskId.get() instanceof Long taskIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "taskId should be Long");
            }
            cancelTask(taskIdLong);
        }
        else if (command.get().equals("taskList")){
            taskList();
        }

    }

    public void createUser(String name , UserPrivilege userPrivilege, String password , boolean isAdmin){
        UserData user =new UserData(UserData.clusterIdCounter.count() , name , userPrivilege  , password , isAdmin);
    }

    public void editUser(long userId ,String name , UserPrivilege userPrivilege, String password , boolean isAdmin){

    }

    public void removeUser(long userId){

    }

    public List<User> userList(){
        return null;
    }

    public void terminateSession(Long sessionId){

    }

    public List<Session> sessionList(){
        return null;
    }

    public void cancelTask(long taskId){

    }
    public List<KTask> taskList(){
        return null;
    }


}

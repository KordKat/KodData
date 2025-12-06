package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.users.User;
import hello1.koddata.sessions.users.UserData;
import hello1.koddata.sessions.users.UserManager;
import hello1.koddata.sessions.users.UserPrivilege;

import java.util.List;

public class UserCommand extends KodFunction<Object>{
    @Override
    public Value<Object> execute() throws KException {
        if (!arguments.containsKey("command")) {
            throw new KException(ExceptionCode.KDE0012, "You need to write command to process user command");
        }
        Value<?> command = arguments.get("command");

        if (command.get().equals("create")) {
            if (!arguments.containsKey("name")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write name to create user");
            }
            Value<?> name = arguments.get("name");
            if (!(name.get() instanceof String nameString)) {
                throw new KException(ExceptionCode.KDE0012, "name should be string");
            }

            if (!arguments.containsKey("maxSession")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write maximumResource to create user");
            }
            if (!arguments.containsKey("maxProcessPerSession")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write maximumResource to create user");
            }
            if (!arguments.containsKey("maxMemoryPerProcess")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write maximumResource to create user");
            }
            if (!arguments.containsKey("maxStorageUsage")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write maximumResource to create user");
            }
            Value<?> maxSession = arguments.get("maxSession");
            Value<?> maxProcessPerSession = arguments.get("maxProcessPerSession");
            Value<?> maxMemoryPerProcess = arguments.get("maxMemoryPerProcess");
            Value<?> maxStorageUsage = arguments.get("maxStorageUsage");
            if (!(maxSession.get() instanceof Integer maxSessionInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxProcessPerSession.get() instanceof Integer maxProcessPerSessionInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxMemoryPerProcess.get() instanceof Integer maxMemoryPerProcessInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxStorageUsage.get() instanceof Integer maxStorageUsageInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            UserPrivilege maximumResourceUP = new UserPrivilege(maxSessionInt, maxProcessPerSessionInt, maxMemoryPerProcessInt, maxStorageUsageInt);

            if (!arguments.containsKey("password")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write password to create user");
            }
            Value<?> password = arguments.get("password");
            if (!(password.get() instanceof String passwordString)) {
                throw new KException(ExceptionCode.KDE0012, "password should be String");
            }
            if (!arguments.containsKey("isAdmin")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write isAdmin to create user");
            }
            Value<?> isAdmin = arguments.get("isAdmin");
            if (!(isAdmin.get() instanceof Boolean isAdminB)) {
                throw new KException(ExceptionCode.KDE0012, "isAdmin should be Logical");
            }
            createUser(nameString, maximumResourceUP, passwordString, isAdminB);
        } else if (command.get().equals("edit")) {
            if (!arguments.containsKey("userId")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write userId to edit user");
            }
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "userId should be Long");
            }
            Value<?> name = arguments.get("name");
            if (!(name.get() instanceof String nameString)) {
                throw new KException(ExceptionCode.KDE0012, "name should be string");
            }

            Value<?> maxSession = arguments.get("maxSession");
            Value<?> maxProcessPerSession = arguments.get("maxProcessPerSession");
            Value<?> maxMemoryPerProcess = arguments.get("maxMemoryPerProcess");
            Value<?> maxStorageUsage = arguments.get("maxStorageUsage");
            if (!(maxSession.get() instanceof Integer maxSessionInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxProcessPerSession.get() instanceof Integer maxProcessPerSessionInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxMemoryPerProcess.get() instanceof Integer maxMemoryPerProcessInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            if (!(maxStorageUsage.get() instanceof Integer maxStorageUsageInt)) {
                throw new KException(ExceptionCode.KDE0012, "maximumResource should be userPrivilege");
            }
            UserPrivilege maximumResourceUP = new UserPrivilege(maxSessionInt, maxProcessPerSessionInt, maxMemoryPerProcessInt, maxStorageUsageInt);

            Value<?> password = arguments.get("password");
            if (!(password.get() instanceof String passwordString)) {
                throw new KException(ExceptionCode.KDE0012, "password should be String");
            }
            Value<?> isAdmin = arguments.get("isAdmin");
            if (!(isAdmin.get() instanceof Boolean isAdminB)) {
                throw new KException(ExceptionCode.KDE0012, "isAdmin should be Logical");
            }
            editUser(userIdLong, nameString, maximumResourceUP, passwordString, isAdminB);
        } else if (command.get().equals("remove")) {
            if (!arguments.containsKey("userId")) {
                throw new KException(ExceptionCode.KDE0012, "You need to write userId to remove user");
            }
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "userId should be Long");
            }
            removeUser(userIdLong);

        } else if (command.get().equals("userlist")) {
            return new Value<>(userList());
        }

        return new Value<>(null);
    }


    public void createUser(String name , UserPrivilege userPrivilege, String password , boolean isAdmin) throws KException {
        UserData user =new UserData(UserData.idCounter.next(), name , userPrivilege  , password , isAdmin);
        Main.bootstrap.getUserManager().createUser(user);
    }

    public void editUser(long userId ,String name , UserPrivilege userPrivilege, String password , Boolean isAdmin) throws KException {
        UserData user = Main.bootstrap.getUserManager().findUser(userId).getUserData();
        if (user == null){
            throw new KException(ExceptionCode.KDE0012, "User does not exist");
        }
        String newName = name == null ? user.name() : name;
        UserPrivilege newUserPrivilege = userPrivilege == null ? user.userPrivilege() : userPrivilege ;
        String newPassword = password == null ? user.password() : password;
        boolean newIsAdmin = isAdmin == null ? user.isAdmin() : isAdmin;

        Main.bootstrap.getUserManager().findUser(userId).setUserData(new UserData(userId , newName , newUserPrivilege , newPassword , newIsAdmin));

    }

    public void removeUser(long userId) throws KException {
        UserData user = Main.bootstrap.getUserManager().findUser(userId).getUserData();
        if (user == null){
            throw new KException(ExceptionCode.KDE0012, "User does not exist");
        }
        Main.bootstrap.getUserManager().removeUser(userId);
    }

    public List<User> userList(){
        return Main.bootstrap.getUserManager().userList();
    }

}

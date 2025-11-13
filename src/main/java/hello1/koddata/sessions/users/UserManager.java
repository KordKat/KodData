package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserManager {
    private Map<Long, User> users;
    private Map<Long, UserData> userDataMap;
    private Map<String, Long> userIdNameMap;
    private Bootstrap bootstrap;
    private File userFile;
    public UserManager(Bootstrap bootstrap){
        users = new ConcurrentHashMap<>();
        userDataMap = new ConcurrentHashMap<>();
        userIdNameMap = new ConcurrentHashMap<>();
        this.bootstrap = bootstrap;
    }

    public User logIn(long userId ,String password){
        if(userDataMap.containsKey(userId)){
            UserData userData = userDataMap.get(userId);
            if(userData.password().equals(password)){
                if(users.containsKey(userId)) return users.get(userId);
                User user;
                if(userData.isAdmin()){
                    user = new Admin(userData, this, bootstrap.getSessionManager());
                }else {
                    user = new User(userData);
                }

                users.put(userId, user);
                return user;
            }
        }
        return null;
    }

    public long getUserId(String username) {
        return userIdNameMap.getOrDefault(username, -1L);
    }

    public void createUser(UserData data) throws KException {
        if(userDataMap.containsKey(data.userId())){
            throw new KException(ExceptionCode.KD00000, "user id already taken");
        }

        this.userDataMap.put(data.userId(), data);

    }

    public User findUser(long userId){
        return users.get(userId);
    }

    public List<User> userList(){
        return users.values().stream().toList();
    }



    public void deleteUser(long userId){
        if(users.containsKey(userId)){
            User user = users.get(userId);
            for(Session session : user.listSessions()){
                session.terminate();
            }
        }
    }

    public void loadAllUserData(File file){
        this.userFile = file;
    }

    public void saveUserData(){

    }

    public void changeUserPassword(long userId, String newPass){
        if(userDataMap.containsKey(userId)){
            userDataMap.computeIfPresent(userId, (k, userData) -> userData.withPassword(newPass));
        }
    }

}

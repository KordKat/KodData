package hello1.koddata.sessions.users;

import hello1.koddata.sessions.Session;

import java.io.File;
import java.util.List;
import java.util.Map;

public class UserManager {
    Map<Long, User> user;
    Map<Long, UserData> userDataMap;


    public User editUser(long userId){return null;}
    public User createUser(){return null;}
    public User findUser(long userId){return null;}
    public User updateUser(long userId){return null;}
    public List<User> userList(){return null;}

    public void changeUserQueue(long userId){}
    public void deleteUser(long userId){}
    public void loadAllUserData(File file){}
    public void changeUserPassword(long userId){}

}

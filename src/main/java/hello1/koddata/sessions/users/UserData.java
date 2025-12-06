package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.concurrent.IdCounter;

import java.util.HashSet;
import java.util.stream.Collectors;

public record UserData(long userId,
                       String name,
                       UserPrivilege userPrivilege,
                       String password,
                       boolean isAdmin) {
    public UserData withPassword(String newPass){
        return new UserData(userId, name, userPrivilege, newPass, isAdmin);
    }

    public static IdCounter idCounter = new IdCounter();

}

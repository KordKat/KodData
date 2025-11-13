package hello1.koddata.sessions.users;

public record UserData(long userId,
                       String name,
                       UserPrivilege userPrivilege,
                       String password,
                       boolean isAdmin) {
    public UserData withPassword(String newPass){
        return new UserData(userId, name, userPrivilege, newPass, isAdmin);
    }
}

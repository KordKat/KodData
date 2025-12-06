package hello1.koddata.sessions.users;

public record UserPrivilege(int maxSession ,
                            int maxProcessPerSession,
                            int maxMemoryPerProcess,
                            int maxStorageUsage) {

}

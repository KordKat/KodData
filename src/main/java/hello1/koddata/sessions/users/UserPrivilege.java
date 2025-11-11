package hello1.koddata.sessions.users;

public record UserPrivilege(int maxSession ,
                            int maxProcessPerSession ,
                            boolean canAccessAdmin,
                            int maxMemoryPerProcess,
                            int maxStorageUsage) {

}

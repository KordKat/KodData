package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.net.NodeStatus;

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

    public static ClusterIdCounter clusterIdCounter = ClusterIdCounter.getCounter(UserData.class, new HashSet<>(Main.bootstrap.getServer().getStatusMap().values()));

}

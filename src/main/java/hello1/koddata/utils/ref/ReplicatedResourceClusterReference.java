package hello1.koddata.utils.ref;

import hello1.koddata.Main;
import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.concurrent.cluster.Replica;
import hello1.koddata.utils.KodResourceNaming;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//consistency
public class ReplicatedResourceClusterReference<T extends Replica> extends UniqueReference<T> implements KodResourceNaming {

    private long latestUpdate;
    private long identifier;
    private static final ClusterIdCounter globalReplicatedResourceClusterReferenceCounter =
            ClusterIdCounter.getCounter(ReplicatedResourceClusterReference.class,
                    new HashSet<>(Main.bootstrap.getServer().getStatusMap().values()));

    public static final ConcurrentMap<String, ReplicatedResourceClusterReference<?>> resources = new ConcurrentHashMap<>();

    public ReplicatedResourceClusterReference(T referent, Sebastian cleaner, long identifier) {
        super(referent, cleaner);
        latestUpdate = System.currentTimeMillis();
        this.identifier = identifier;
        resources.put(getResourceName(), this);
    }

    public ReplicatedResourceClusterReference(T referent, Sebastian cleaner){
        this(referent, cleaner, globalReplicatedResourceClusterReferenceCounter.count());
    }

    public long getLatestUpdate() {
        return latestUpdate;
    }

    public long getReferenceIdentifier(){
        return identifier;
    }

    @Override
    public String getResourceName() {
        return ClusterReference.class.getName() + "::" + super.get().getClass().getName() + "::" + identifier;
    }

    public void updateReplica(){
        get().update(get().getConsistencyCriteria());
    }

}

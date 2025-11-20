package hello1.koddata.utils.ref;

import hello1.koddata.Main;
import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.concurrent.cluster.ResourceLock;

import java.net.InetSocketAddress;
import java.util.HashSet;

public class ClusterReference<T> extends UniqueReference<T> {

    private boolean existInNode;
    private InetSocketAddress ownerNode;
    private long identifier;
    private static final ClusterIdCounter globalClusterReferenceCounter =
            ClusterIdCounter.getCounter(ClusterReference.class,
                    new HashSet<>(Main.bootstrap.getServer().getStatusMap().values()));

    public ClusterReference(Sebastian cleaner, InetSocketAddress isa, long id) {
        super(null, cleaner);
        this.identifier = id;
        this.ownerNode = isa;
    }

    public ClusterReference(T referent, Sebastian cleaner){
        super(referent, cleaner);
        this.identifier = globalClusterReferenceCounter.count();
        this.ownerNode = Main.bootstrap.getServer().getDataTransferServerInetSocketAddress();
    }

    public long getReferenceIdentifier(){
        return identifier;
    }

    public String getResourceIdentifier() {
        return ClusterReference.class.getName() + "::" + super.get().getClass().getName() + "::" + identifier;
    }

    public void toThisNode(){

    }

    public void sendToNode(InetSocketAddress isa){

    }

}

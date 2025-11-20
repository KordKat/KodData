package hello1.koddata.concurrent.cluster;

public interface Replica {

    ConsistentCriteria getConsistencyCriteria();

    void update(ConsistentCriteria latest);

}

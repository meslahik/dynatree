package ch.usi.dslab.lel.dynastarv2.sample;

import ch.usi.dslab.lel.dynastarv2.OracleStateMachine;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;

/**
 * Created by longle on 22/11/15.
 */
public class AppOracle extends OracleStateMachine {
    public AppOracle(int serverId, String systemConfig, String partitionsConfig) {
        super(serverId, systemConfig, partitionsConfig);
    }

    public static void main(String args[]) {
        String systemConfigFile;
        String partitionConfigFile;
        int replicaId;
        if (args.length == 3) {
            replicaId = Integer.parseInt(args[0]);
            systemConfigFile = args[1];
            partitionConfigFile = args[2];
            AppOracle oracle = new AppOracle(replicaId, systemConfigFile, partitionConfigFile);
            oracle.runStateMachine();
        } else {
            System.out.println("USAGE: AppOracle | replicaId| systemConfigFile | partitionConfigFile");
            System.exit(1);
        }
    }

    @Override
    public PRObject createObject(ObjId id, Object value) {
        return null;
    }
}

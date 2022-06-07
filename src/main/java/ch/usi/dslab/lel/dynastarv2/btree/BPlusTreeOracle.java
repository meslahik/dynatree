package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.CPUMonitor;
import ch.usi.dslab.lel.dynastarv2.OracleStateMachine;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;

/**
 * Created by meslahik on 11.09.17.
 */
public class BPlusTreeOracle extends OracleStateMachine {
    CPUMonitor cpuMonitor;
    public BPlusTreeOracle(int serverId, String systemConfig, String partitionsConfig) {
        super(serverId, systemConfig, partitionsConfig);
    }

    public static void main(String args[]) {
        try {
            String systemConfigFile;
            String partitionConfigFile;
            int oracleId;
            if (args.length == 9) {
                oracleId = Integer.parseInt(args[0]);
                systemConfigFile = args[1];
                partitionConfigFile = args[2];
                BPlusTreeOracle oracle = new BPlusTreeOracle(oracleId, systemConfigFile, partitionConfigFile);
                boolean gatherer = Boolean.parseBoolean(args[3]);
                if (gatherer) {
                    int i = 4;
                    String gathererHost = args[i];
                    int gathererPort = Integer.parseInt(args[i + 1]);
                    String gathererDir = args[i + 2];
                    int gathererDuration = Integer.parseInt(args[i + 3]);
                    int gathererWarmup = Integer.parseInt(args[i + 4]);
//                    DataGatherer.configure(gathererDuration, gathererDir, gathererHost, gathererPort, gathererWarmup);
                    oracle.setupMonitoring(gathererHost, gathererPort, gathererDir, gathererDuration, gathererWarmup);
                }
                oracle.runStateMachine();
            } else {
                System.out.println("USAGE: BPlusTreeOracle oracleId systemConfigFile partitionConfigFile gatherer gathererHost gathererPort gathererDir gathererDuration gathererWarmup");
                System.exit(1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Override
    public PRObject createObject(ObjId id, Object value) {
        return null;
    }
}

package ch.usi.dslab.lel.dynastarv2;
/*
 * ScalableSMR - A library for developing Scalable services based on SMR
 * Copyright (C) 2017, University of Lugano
 *
 *  This file is part of ScalableSMR.
 *
 *  ScalableSMR is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

import ch.usi.dslab.bezerra.mcad.ClientMessage;
import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.monitors.OracleMovePassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastarv2.command.CmdId;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.CommandType;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.messages.MessageType;
import ch.usi.dslab.lel.dynastarv2.partitioning.MetisPartitioner;
import ch.usi.dslab.lel.dynastarv2.partitioning.Partitioner;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by longle on 17.07.17.
 */
public abstract class OracleStateMachine extends StateMachine {

    private OracleMovePassiveMonitor repartitioningLogger;
    private ThroughputPassiveMonitor queryTPMonitor;
    private ThroughputPassiveMonitor hintTPMonitor;
    private ThroughputPassiveMonitor exchangedObjsCountMonitor;
    private AtomicInteger totalChangesCount = new AtomicInteger(0);
    private AtomicInteger changesFromLastRepartition = new AtomicInteger(0);
        private int REPARTITIONING_THRESHOLD = 3000;
//    private int REPARTITIONING_THRESHOLD = 30;
//                private boolean REPARTITIONING_ENABLED = false;
    private boolean REPARTITIONING_ENABLED = true;
    private int PARTITIONING_TIME_INTERVAL = 120;
    int PARTITIONING_NUM_MAX = 2;
//    int partitioning_num = 0;
    private Partitioner partitioner;
    private AtomicInteger idGenerator = new AtomicInteger(0);
    private AtomicInteger nextCommandId = new AtomicInteger(0);
    private boolean isDoingPartitioning = false;

    float numCmds;
    float numSplitCmds = 0;
    float numRoutingCmds = 0;

    public OracleStateMachine(int replicaId, String systemConfig, String partitionsConfig) {
        super(replicaId, systemConfig, partitionsConfig, null);
        this.logger = LoggerFactory.getLogger(OracleStateMachine.class);
        MDC.put("ROLE", replica.getPartition().getType() + "-" + replica.getPartitionId() + "/" + replicaId);
        this.partitioner = new MetisPartitioner(this);
        this.setLogger(logger);
        System.setErr(System.out);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (numCmds != 0) {
                    float percentRoutingCmds = (numRoutingCmds / numCmds) * 100;
                    float percentSplitCmds = (numSplitCmds / numCmds) * 100;
                    System.out.println("numCmds: " + numCmds + ", numRoutingCmds: " + numRoutingCmds + ", numSplitCmds: " + numSplitCmds + ", percent of routing cmds: " + percentRoutingCmds + "%, percent of split requests: " + percentSplitCmds +"%");
                }
            }
        }, 1000, 1000);
    }

    public void setupMonitoring(String gathererHost, int gathererPort, String fileDirectory, int gathererDuration, int warmupTime) {
        super.setupMonitoring(gathererHost, gathererPort, fileDirectory, gathererDuration, warmupTime);
        queryTPMonitor = new ThroughputPassiveMonitor(replicaId, "oracle_query", false);
        repartitioningLogger = new OracleMovePassiveMonitor(replicaId, "oracle_repartitioning", false);
    }


    @Override
    public void runStateMachine() {
        // added by mojtaba
        Thread thread = new Thread(() -> {
            for (int repartitioning_count = 0; repartitioning_count < PARTITIONING_NUM_MAX; repartitioning_count++) {
                try {
                    Thread.sleep(PARTITIONING_TIME_INTERVAL * 1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                doRepartition();
            }
        });
        thread.start();

        readySemaphore.release();
        // add by mojtaba
//        long partitioningStartTime = System.currentTimeMillis();
        while (replica.running) {
            logger.debug("taking command from queue, queue size {}", replica.executionQueue.size());
            Command cmd = getNextCommandToExecute();
            logger.debug("cmd {} is ready to execute", cmd.getId());

//            // added by mojtaba
//            long time = System.currentTimeMillis();
//            long diff = (time - partitioningStartTime)/1000;
//            logger.debug("time for starting repartitioning? time diff from previous repartition {}", diff);
//            if (diff >= PARTITIONING_TIME_INTERVAL) {
//                doRepartition();
//                partitioningStartTime = System.currentTimeMillis();
//            }

            boolean done = attemptToExecuteRepartitionCommand(cmd);
            if (done) continue;

            Message reply = executeCommand(cmd);
            if (reply != null) {
                replica.sendReply(reply, cmd);
            }
        }
    }

    private boolean shouldIncludeOracle(Command command) {
        command.rewind();
        MessageType type = (MessageType) command.getNext();
        return (type instanceof CommandType && (type == CommandType.CREATE || type == CommandType.DELETE) || (command.getReservedObjects() != null));
    }

    public Message executeCommand(Command command) {
        if (queryTPMonitor != null) queryTPMonitor.incrementCount();
        return handleCommand(command);
    }

    private boolean attemptToExecuteRepartitionCommand(Command command) {
        if (!command.isRepartitioning()) return false;
        int partitioningVersion = (int) command.getItem(0);
        if (partitioningVersion < this.objectGraph.getPartitioningVersion())
            return true; // receive old partitioning map
        logger.info("applying new partitioning #{}", partitioningVersion);
        if (repartitioningLogger != null)
            repartitioningLogger.logMessage("REPARTITIONING " + this.objectGraph.getNodesCount() + " " + partitioningVersion);
        else
            System.out.println("oracle partitioning logger disabled");
        this.objectGraph.applyNewPartitioning(partitioningVersion);
        isDoingPartitioning = false;
        return true;
    }

    private Message handleCommand(Command command) {
        command.rewind();
        MessageType cmdType = (MessageType) command.getNext();
        Prophecy prophecy = new Prophecy();
        int partId = -1;
        final Set<ObjId> objIds = new HashSet<>();
        if (cmdType == CommandType.CREATE) {
            Set<PRObject> objs = (Set<PRObject>) command.getNext();
            objs.forEach(prObject -> {
                if (prObject.getId() == null) prObject.setId(genId());
                objIds.add(prObject.getId());
            });
            Set<ObjId> dependencies = Utils.extractObjectId(command);
            partId = getPartitionWithMostObjects(dependencies, objIds);
            if (partId == -1) partId = Utils.getObjectPlacement(objIds);
            int finalPartId = partId;
            objIds.forEach(objId -> prophecy.add(objId, finalPartId));
            dependencies.forEach(objId -> prophecy.add(objId, objectGraph.getNode(objId).getPartitionId()));
        } else {
            objIds.addAll(Utils.extractObjectId(command));
            for (ObjId oid : objIds) {
                PRObjectNode node = objectGraph.getNode(oid);
                if (node != null) {
                    logger.info("cmd {} updated reserved object {} on partition {}", command.getId(), node, node.getPartitionId());
                    node.setReserved(false);
                    partId = node.getPartitionId();
                    prophecy.add(node.getId(), partId);
                }
            }

        }

        if (command.getReservedObjects() != null) {
            numCmds++;
            numSplitCmds++;
            Set<PRObject> objs = command.getReservedObjects();
            for (PRObject object : objs) {
                if (object.getId() == null) object.setId(genId());
            }
        }
        else {
            numCmds++;
            numRoutingCmds++;
        }

        int destPartId = (cmdType == CommandType.CREATE) ? partId : getPartitionWithMostObjects(objIds);
//        logger.info("destpartId: {}, command {}, prophecy {}", destPartId, command.toFullString(), prophecy);
        boolean shouldIncludeOracle = shouldIncludeOracle(command);
        command.setObjectMap(prophecy.objectMap);
        forwardCommand(command, destPartId, shouldIncludeOracle);
        return new Message(prophecy);
    }

    private ObjId genId() {
        int newId = idGenerator.incrementAndGet();
        while (this.objectGraph.getNode(new ObjId(newId)) != null) newId = idGenerator.incrementAndGet();
        return new ObjId(newId);
    }

    public void updateMemory(Command command) {
        try {
            // special case for app comd creating obj
            if (command.getReservedObjects() != null) {
                Set<PRObject> objs = command.getReservedObjects();
                int destPart = command.getDestinations().stream().filter(i -> i.getType().equals("PARTITION")).collect(Collectors.toList()).get(0).getId();
                for (PRObject object : objs) {
                    PRObjectNode node = objectGraph.getNode(object.getId());
                    if (node != null) {
                        node.update(destPart);
                    } else {
//                    totalChangesCount.incrementAndGet();
                        changesFromLastRepartition.incrementAndGet();
                        logger.debug("Change count by create reserved objects {}", totalChangesCount.get());
                        node = new PRObjectNode(object.getId(), destPart);
                        node.setReserved(true);
                        this.objectGraph.addNode(node);
                    }
                }
//            checkForRepartition();
                unlockObjects(command.getId());
                return;
            }
            CommandType cmdType = (CommandType) command.getNext();

            switch (cmdType) {
                case CREATE: {
                    logger.debug("cmd {} - updating memory as a CREATE command", command.getId());
                    Set<PRObject> objs = (Set<PRObject>) command.getNext();

                    int destPart = command.getDestinations().stream().filter(i -> i.getType().equals("PARTITION")).collect(Collectors.toList()).get(0).getId();
                    for (PRObject object : objs) {
                        PRObjectNode node = objectGraph.getNode(object.getId());
                        if (node != null) {
                            node.update(destPart);
                        } else {
                            totalChangesCount.incrementAndGet();
                            changesFromLastRepartition.incrementAndGet();
                            logger.debug("Change count by create {}", totalChangesCount.get());
                            node = new PRObjectNode(object.getId(), destPart);
                            this.objectGraph.addNode(node);
                        }
                    }
                    break;
                }
                case DELETE: {
                    logger.debug("cmd {} - updating memory as a DELETE command", command.getId());
                    ObjId oid = (ObjId) command.getNext();
                    PRObjectNode node = objectGraph.getNode(oid);
                    if (node != null) {
                        totalChangesCount.incrementAndGet();
                        objectGraph.removeNode(oid);
                        logger.debug("Change count by delete {}", totalChangesCount.get());
                        changesFromLastRepartition.incrementAndGet();
                    }
                    break;
                }
                default: {
                }
            }
            //unlock object accessed by the command
            unlockObjects(command.getId());
//            checkForRepartition();
        } catch (java.lang.ClassCastException e) {
            System.out.println("Oracle " + command);
            e.printStackTrace();
        }
    }

    // add by mojtaba
    private void doRepartition() {
        if (Partition.getPartitionList().size() > 1 && REPARTITIONING_ENABLED && !isDoingPartitioning) {
            isDoingPartitioning = true;
            logger.info("Change {} exceeds threshold. Starting new repartitioning. Current version #{}", changesFromLastRepartition.get(), this.objectGraph.getPartitioningVersion());
            changesFromLastRepartition.set(0);
            this.partitioner.repartition(this.objectGraph, Partition.getPartitionsCount());
        }
    }

    private void checkForRepartition() {
        if (Partition.getPartitionList().size() > 1 && REPARTITIONING_ENABLED && changesFromLastRepartition.get() >= REPARTITIONING_THRESHOLD && !isDoingPartitioning) {
            isDoingPartitioning = true;
            logger.info("Change {} exceeds threshold. Starting new repartitioning. Current version #{}", changesFromLastRepartition.get(), this.objectGraph.getPartitioningVersion());
            changesFromLastRepartition.set(0);
            this.partitioner.repartition(this.objectGraph, Partition.getPartitionsCount());
        }
    }

    public void handleFeedback(int partitionId, List<PRObjectGraph.Edge> edges) {
        logger.info("receiving feedback from partition {} - {}/{}", partitionId, edges.size(), edges);
//        if (repartitioningLogger != null)
//            repartitioningLogger.logMessage("REPARTITIONING::FEEDBACK::" + partitionId + "::" + edges.size());
        int count = this.objectGraph.updateGraphEdges(edges);
        totalChangesCount.addAndGet(count);
        changesFromLastRepartition.addAndGet(count);
        logger.info("adding {} changes based on feedback from partition {}, total {}, last changes {}", count, partitionId, totalChangesCount.get(), changesFromLastRepartition.get());
//        checkForRepartition();
    }

    public void handleMapReceivingSignal(Message m) {
        m.rewind();
        DynaStarMessageType type = (DynaStarMessageType) m.getNext();
        int partitionId = (int) m.getNext();
        int partitioningId = (int) m.getNext();
        if (partitioningId < this.objectGraph.getPartitioningVersion()) return; //receive signal for old partitioning.
        logger.debug("Received signal from partition {} for partitioning #{}, adding to queue", partitionId, partitioningId);
        this.partitioner.queueSignal(partitionId, partitioningId);
        logger.debug("Receiving enough signal for partitioning #{}? {}", partitioningId, this.partitioner.isSignalsFullfilled(partitioningId));
        if (this.partitioner.isSignalsFullfilled(partitioningId)) {
            Command partitionCommand = new Command(partitioningId);
            ClientMessage wrapper = new ClientMessage(DynaStarMessageType.ORACLE_PARTITION, partitionCommand);
            CmdId cmdid = new CmdId(this.replicaId, this.getNextCommandId());
            partitionCommand.setId(cmdid);
            partitionCommand.setPartitioningVersion(this.objectGraph.getPartitioningVersion());
            Set<Partition> dests = Partition.getAllPartition();
            logger.debug("Multicasting repartitioning #{} command {}", partitioningId, partitionCommand);
            partitionCommand.setDestinations(dests);
            if (replica.isPartitionMulticaster()) {
                logger.debug("Oracle {}/{} multicast #{}", replicaId, getPartitionId(), partitioningId);
                this.replica.multicast(dests, wrapper);
            }
        }
    }

    @Override
    public void handlePartitionCommand(Command command) {
        logger.debug("handling partitioning command {}", command);
        replica.queueNextAwaitingExecution(command);
    }

    public long getNextCommandId() {
        return nextCommandId.incrementAndGet();
    }

    public static class Prophecy implements Serializable {
        public boolean sync = false;
        Map<Integer, Set<ObjId>> objectMap = new HashMap<>();
        int destPartId;
        int moveCount = 0;

        Prophecy() {
        }

        public void add(ObjId objId, int partitionId) {
            Set<ObjId> objIds = objectMap.get(partitionId);
            if (objIds == null) {
                objIds = new HashSet<>();
                objectMap.put(partitionId, objIds);
            }
            objIds.add(objId);
        }

        public int getFirstPartition() {
            return objectMap.keySet().iterator().next();
        }

        public void setDestinationPartition(int destPartId) {
            this.destPartId = destPartId;
        }

        public void setMoveCount(int moveCount) {
            this.moveCount = moveCount;
        }

        @Override
        public String toString() {
            StringBuffer str = new StringBuffer("[");
            this.objectMap.keySet().forEach(key -> str.append(key + ":" + this.objectMap.get(key) + ", "));
            str.append("]");
            str.append("- Destination: ");
            str.append(destPartId);
            str.append("- Move count: ");
            str.append(moveCount);
            return str.toString();
        }
    }
}

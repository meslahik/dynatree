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

import ch.usi.dslab.bezerra.mcad.*;
import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.CPUEmbededMonitorJavaMXBean;
import ch.usi.dslab.lel.dynastarv2.command.CmdId;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.CommandType;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.messages.MessageType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * Created by longle on 17.07.17.
 */
public abstract class StateMachine {
    protected static Logger logger = null;
    protected static StateMachine instance;
    public PRObjectGraph objectGraph;
    protected Semaphore readySemaphore;
    protected MulticastServer multicastServer;
    protected MulticastClient multicastClient;
    protected int replicaId;
    protected int partitionId;
    protected Replica replica;
    protected boolean loggingEnabled = false;
    protected ConcurrentHashMap<CmdId, List<ObjId>> objectsInProcessing = new ConcurrentHashMap<>();
    protected String gathererHost = null;
    protected int gathererPort;
    protected String fileDirectory;
    protected int gathererDuration;
    protected int warmupTime;
    protected boolean monitoringInited = false;
    protected Map<String, Set<ObjId>> secondaryIndex = new HashMap<>();

    CPUEmbededMonitorJavaMXBean cpuMoniter;
    //    private List<Command> waitingCommandQueue = Collections.synchronizedList(new LinkedList<Command>());
    private List<Command> waitingCommandQueue = new LinkedList<Command>();
    private ConcurrentHashMap<Command, CmdId> waitingCommandMap = new ConcurrentHashMap<>();
    private Set<CmdId> tmp = Collections.synchronizedSet(new HashSet<CmdId>());

    public StateMachine(int replicaId, String systemConfig, String partitionsConfig, DeliveryMetadata checkpoint) {
        this.instance = this;
//        CodecUncompressedKryo.registerClassInSerializationIndex(CmdId.class);
//        CodecUncompressedKryo.registerClassInSerializationIndex(ObjId.class);
        this.readySemaphore = new Semaphore(0);
        this.replicaId = replicaId;
        this.multicastServer = MulticastClientServerFactory.getServer(replicaId, systemConfig);

//        this.multicastClient = MulticastClientServerFactory.getClient(replicaId+99990, systemConfig);
        this.replica = Replica.createReplica(multicastServer, partitionsConfig);
        this.partitionId = replica.getPartitionId();
        this.objectGraph = PRObjectGraph.getInstance(partitionId);
        this.multicastServer.getMulticastAgent().provideMulticastCheckpoint(checkpoint);
//        if (replicaId % replica.getPartition().getGroup().getMembers().size() == 0) {
        this.setLogging(true);
        System.out.println("Replica #" + replicaId + " is logging for PARTITION #" + replica.getPartitionId());
//        }
    }

    public static StateMachine getMachine() {
        return instance;
    }

    public static int getPartitionId() {
        return instance.replica.getPartitionId();
    }

    public static boolean isCreateWithGather(Command command) {
        if (command.getItem(0) != null && command.getItem(0) instanceof CommandType
                && command.getItem(0) == CommandType.CREATE && command.getItem(2) != null) return true;
        return false;
    }

    public void setupMonitoring(String gathererHost, int gathererPort, String fileDirectory, int gathererDuration, int warmupTime) {
        this.gathererHost = gathererHost;
        this.gathererPort = gathererPort;
        this.fileDirectory = fileDirectory;
        this.warmupTime = warmupTime;
        this.gathererDuration = gathererDuration;
        DataGatherer.configure(gathererDuration, fileDirectory, gathererHost, gathererPort, warmupTime);
//        cpuMoniter = new CPUEmbededMonitorJavaMXBean(replicaId, isOracle() ? "ORACLE" : "PARTITION");
//        cpuMoniter.startLogging();

    }

    public int getReplicaId() {
        return replicaId;
    }

    public Semaphore getReadySemaphore() {
        return readySemaphore;
    }

    public abstract void runStateMachine();

    public void setLogging(boolean logging) {
        this.loggingEnabled = logging;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
        this.replica.setLogger(logger);
    }

    protected CmdId isConflicted(Command command, Map<Command, CmdId> waitingList) {
        logger.debug("cmd {} examize if executable", command.getId());
        try {
            Set<ObjId> objIds = Utils.extractObjectId(command);
            if (objectsInProcessing.keySet().size() > 0) {
                logger.debug("cmd {} examize agains objs in object queue {}", command.getId(), objectsInProcessing.keySet());
                for (Map.Entry<CmdId, List<ObjId>> entry : objectsInProcessing.entrySet()) {
                    if (!entry.getKey().equals(command.getId())) {
                        CmdId cmdIdInProcessing = entry.getKey();
                        List tmp = entry.getValue();
                        for (ObjId objId : objIds) {
                            if (tmp != null && tmp.contains(objId)) {
                                logger.debug("cmd {} uses objs in object queue of {}:{}", command.getId(), cmdIdInProcessing, objectsInProcessing.get(cmdIdInProcessing));
                                return cmdIdInProcessing;
                            }
                        }
                    }
                }
            }

            if (waitingList.size() == 0) return null;
            logger.debug("cmd {} examize agains objs in command queue", command.getId());
            for (Map.Entry<Command, CmdId> entry : waitingList.entrySet()) {
                Command cmd = entry.getKey();
                if (cmd.isRepartitioning()) return cmd.getId();
                if (!command.getId().equals(cmd.getId()) && !entry.getValue().equals(command.getId())) {
                    Set<ObjId> cmdObjIds = Utils.extractObjectId(cmd);
                    if (objIds.stream().anyMatch(cmdObjIds::contains)) {
                        logger.debug("cmd {} uses objs in object queue of queued {}:{}", command.getId(), cmd.getId(), cmdObjIds);
                        return cmd.getId();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("====ERROR===CMD:" + command.getId());
            System.exit(1);
        }

        return null;
    }

    public Command getNextCommandToExecute() {
        Command cmd = null;
        boolean takeCmdInQueue = false;
        while (true) {
            int waitingTime = 1000;
            if (waitingCommandQueue.size() > 0) waitingTime = 1;
            logger.debug("delivered commands queue {}, waiting queue {}", replica.executionQueue, waitingCommandQueue);
            // normal flow, prefer cmd in execution queue
            cmd = replica.takeNextAwaitingExecution(waitingTime);
            if (cmd == null && waitingCommandQueue.size() > 0) {
                logger.trace("no command in queue, put back waiting list {}", waitingCommandMap);
                this.replica.putBackAwaitingExecution(waitingCommandMap, waitingCommandQueue);
                continue;
            } else if (cmd == null) {
                logger.trace("no command in queue, waiting list also empty, try next tick");
                continue;
            }

            // here we need to check again if any command in queue has wrong version number, beside
            // checking right after deliver it
            if (cmd.getPartitioningVersion() != objectGraph.getPartitioningVersion() && !isOracle() && !cmd.isSSMRExecution()) {
                logger.debug("cmd {} has old version number {}, current is {}, request retry, checked in statemachine", cmd.getId(), cmd.getPartitioningVersion(), objectGraph.getPartitioningVersion());
                replica.sendReply(new Message("INVALIDATE_CACHE", objectGraph.getPartitioningVersion()), cmd);
                unlockObjects(cmd.getId());
                continue;
            }

            CmdId conflicted = isConflicted(cmd, waitingCommandMap);
            if (conflicted == null) {
                logger.debug("cmd {} is executable. Put back blocked cmd to queue {}", cmd.getId(), waitingCommandQueue);
                if (waitingCommandQueue.size() > 0)
                    this.replica.putBackAwaitingExecution(waitingCommandMap, waitingCommandQueue);
                cmd.rewind();
                break;
            }
            waitingCommandMap.put(cmd, conflicted);
            waitingCommandQueue.add(cmd);
            logger.debug("cmd {} is NOT executable, Put to waiting list to queue {} ", cmd.getId(), waitingCommandQueue);
        }
        return cmd;
    }

    public abstract PRObject createObject(ObjId id, Object value);

    public void lockObjects(Set<ObjId> objIdsToLock, CmdId cmdId) {
        logger.debug("cmd {} going to lock those objs {}", cmdId, objIdsToLock);
        List<ObjId> objIds = objectsInProcessing.get(cmdId);
        if (objIds == null) {
            objIds = Collections.synchronizedList(new LinkedList<>());
            objectsInProcessing.put(cmdId, objIds);
        }
        objIds.addAll(objIdsToLock);
    }


    public void unlockObjects(CmdId cmdId) {
        if (objectsInProcessing.get(cmdId) != null) {
            logger.debug("cmd {} going to unlock those objs {}", cmdId, objectsInProcessing.get(cmdId));
            objectsInProcessing.remove(cmdId);
        }
    }

    protected Set<ObjId> getMissingNodes(Command command) {
        Set<ObjId> objIds = Utils.extractObjectId(command);
        return this.objectGraph.getMissingNodes(objIds);
    }

    protected Set<ObjId> getMissingObjects(Set<ObjId> objIds) {
        return this.objectGraph.getMissingNodes(objIds);
    }

    protected Set<ObjId> getMissingObjects(Command command) {
        Set<ObjId> objIds = Utils.extractObjectId(command);
        return this.objectGraph.getMissingObjects(objIds);
    }

    protected boolean checkForNodesAvailability(Command command, String failedMessage) {
        if (isLocalCommand(command) && !isCreateWithGather(command)) return false;
        Set<ObjId> missingObjs = getMissingNodes(command);
        if (missingObjs.size() != 0) {
            logger.debug("cmd {} Can't find nodes {} in this partition or its neighbor for executing, tell client to retry", command.getId(), missingObjs);
            replica.sendReply(new Message(failedMessage, missingObjs), command);
            return true;
        }
        return false;
    }

    protected boolean checkForObjectsAvailability(Command command, String failedMessage) {
        Set<ObjId> missingObjs = getMissingObjects(command);
        if (missingObjs.size() != 0) {
            logger.debug("cmd {} Can't find objects {} in this partition, tell client to retry", command.getId(), missingObjs);
            replica.sendReply(new Message(failedMessage, missingObjs), command);
            return true;
        }
        return false;
    }

    protected boolean shouldMove(Command command) {
        command.rewind();
        Set<ObjId> objIds;
        MessageType type = (MessageType) command.getNext();
        if ((type instanceof CommandType) && ((type == CommandType.CREATE))) {
            objIds = (command.getItem(2) != null) ? (Set<ObjId>) command.getItem(2) : new HashSet<>(); // set of dependent objects
        } else {
            objIds = Utils.extractObjectId(command);
        }
        List<ObjId> tmp = new ArrayList<>(objIds);

        for (int i = 0; i < tmp.size() - 1; i++) {
            PRObjectNode tmpI = this.objectGraph.getNode(tmp.get(i));
            PRObjectNode tmpJ = this.objectGraph.getNode(tmp.get(i + 1));
            if ((tmpI == null) || tmpJ == null ||
                    ((tmpI.getPartitionId() != tmpJ.getPartitionId()))) {
                return true;
            }
        }
        return false;
    }

    protected boolean isLocalCommand(Command command) {
        command.rewind();
        MessageType type = (MessageType) command.getNext();
        return ((type instanceof CommandType) && ((type == CommandType.CREATE) || (type == CommandType.READ)));
    }

    protected int getPartitionWithMostObjects(Set<ObjId> objIds) {
        return getPartitionWithMostObjects(objIds, null);
    }

    protected int getPartitionWithMostObjects(Set<ObjId> objIds, Set<ObjId> newObjIds) {
        Map<Integer, Integer> map = new HashMap<>();
        int partition = -1;
        int max = -1;
        for (ObjId objId : objIds) {
            if (newObjIds != null && newObjIds.contains(objId)) continue;

            if (this.objectGraph.getNode(objId) == null) {
                logger.info("NOPARTITION - objId: {} - objIds: {}", objId, objIds);
                continue;
            }
            int partitionId = this.objectGraph.getNode(objId).getPartitionId();
            if (map.get(partitionId) == null) map.put(partitionId, 0);
            int count = map.get(partitionId) + 1;
            if (max < count) {
                max = count;
                partition = partitionId;
            }
            map.put(partitionId, count);
        }
        if (partition == -1)
            logger.info("NOPARTITION - objIds: {}", objIds);
        return partition;
    }

    protected void forwardCommand(Command command, int destPartId, boolean shouldIncludeOracle) {

        // lock objects in the command
        Set<ObjId> objIds = new HashSet<>(Utils.extractObjectId(command));
        if (command.getReservedObjects() != null) {
            Set<ObjId> reservedObjIds = command.getReservedObjects().stream().map(x -> x.getId()).collect(Collectors.toSet());
            logger.info("cmd {} has reserved objects {}", command.getId(), reservedObjIds);
            objIds.addAll(reservedObjIds);
        }

        Set<Partition> multicastDestPart = new HashSet<>();
        multicastDestPart.add(Partition.getPartition(destPartId));
        if (shouldIncludeOracle) {
            multicastDestPart.addAll(Partition.getOracleSet());
            lockObjects(objIds, command.getId());
        }

        Set<Partition> dests = new HashSet<>(command.getDestinations());
        dests.add(Partition.getPartition(destPartId));
        command.setDestinations(multicastDestPart);
        command.t_oracle_finish_excecute = System.currentTimeMillis();
        command.setPartitioningVersion(this.objectGraph.getPartitioningVersion());
        ClientMessage wrapper = new ClientMessage(DynaStarMessageType.FORWARD, command);
        if (replica.isPartitionMulticaster(command)) {
            logger.debug("cmd {} forward to partitions {}, cmd destination {}, cmd updated version", command.getId(), destPartId, command.getDestinations(), command.getPartitioningVersion());
            replica.multicast(multicastDestPart, wrapper);
        }

    }


    protected boolean isOracle() {
        return this.replica.getPartition().isOracle();
    }

    public Replica getReplica() {
        return replica;
    }

    public abstract void handlePartitionCommand(Command command);

    protected void addToSecondaryIndex(String key, ObjId objId){
        Set<ObjId> oids = secondaryIndex.get(key);
        if (oids == null) {
            oids = new HashSet<>();
            secondaryIndex.put(key, oids);
        }
        oids.add(objId);
    }
    //    public abstract void handleObjectGatherMessage(Command command);
}

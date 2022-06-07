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
import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastarv2.command.CmdId;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.CommandType;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.messages.MessageType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by longle on 17.07.17.
 */
public abstract class PartitionStateMachine extends StateMachine {

    public final static String RUNNING_MODE_DYNASTAR = "DYNASTAR";
    public final static String RUNNING_MODE_SSMR = "SSMR";
    public ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private AtomicInteger changesCount = new AtomicInteger(0);
    private int FEEDBACK_INTERVAL = 700;
//    private int FEEDBACK_INTERVAL = 10;
    private int FEEDBACK_TIME_INTERVAL = 10;
    private AtomicInteger nextCommandId = new AtomicInteger(0);
    private List<CmdId> waitingThreads = Collections.synchronizedList(new LinkedList<CmdId>());
    private Map<CmdId, QueuedCommand> queuedCommand = new ConcurrentHashMap<>();
//        private boolean REPARTITIONING_ENABLED = false;
    private boolean REPARTITIONING_ENABLED = true;
    private ThroughputPassiveMonitor moveTPMonitor;

    public PartitionStateMachine(int replicaId, String systemConfig, String partitionsConfig) {
        super(replicaId, systemConfig, partitionsConfig, null);
        this.logger = LoggerFactory.getLogger(PartitionStateMachine.class);
        MDC.put("ROLE", replica.getPartition().getType() + "-" + replica.getPartitionId() + "/" + replicaId);
        this.setLogger(logger);
        System.setErr(System.out);
    }

    public Map<CmdId, QueuedCommand> getQueuedCommand() {
        return queuedCommand;
    }

    public void setupMonitoring(String gathererHost, int gathererPort, String fileDirectory, int gathererDuration, int warmupTime) {
        super.setupMonitoring(gathererHost, gathererPort, fileDirectory, gathererDuration, warmupTime);
        moveTPMonitor = new ThroughputPassiveMonitor(replicaId, "partition_move", false);
    }

    @Override
    public void runStateMachine() {
        readySemaphore.release();
        long partitioningStartTime = System.currentTimeMillis();
        while (replica.running) {
//            logger.debug("taking command from queue, queue size {}", replica.executionQueue.size());
            Command command = getNextCommandToExecute();
//            logger.debug("cmd {} is ready to execute", command.toFullString());


            // TIMELINE STUFF
            command.t_partition_dequeued = System.currentTimeMillis();

            boolean done = false;

            if (command.isSSMRExecution()) {
                attempToExecuteSSMRCommand(command);
                continue;
            }

            Object attempt = attemptToExecuteGatherCommand(command);
            if (attempt instanceof CompletableFuture) {
                logger.debug("cmd {} is an async one, going to execute it, current active threads count {}, queue {}", command.getId(), ((ThreadPoolExecutor) executorService).getActiveCount(), waitingThreads);
                // command is a gather command
                // and this is source replica, wait for getting back obj
                ((CompletableFuture) attempt).thenAccept(o -> {
                    logger.debug("cmd {} finish async execution, thread name {}", command.getId(), Thread.currentThread().getName());
                    waitingThreads.remove(command.getId());
                }).exceptionally(o -> {
                    ((Exception) o).printStackTrace();
                    return null;
                }).exceptionally(e -> {
                    ((Exception) e).printStackTrace();
                    return 0;
                });
                continue;
            }
            if (attempt instanceof Boolean && (boolean) attempt) {
                // command is a gather command
                // and this is destination replica, don't do any thing
                continue;
            }

            attempt = attemptToExecuteRepartitionCommand(command);
            if (attempt instanceof Boolean && (boolean) attempt) {
                continue;
            }

            //cmd is not a gather command
            done = checkForNodesAvailability(command, "RETRY");
            if (done) continue;

            logger.debug("cmd {} processing app command {}", command.getId(), command);

            int newEdges = updateObjectGraphAsClientCommand(command);
            int count = changesCount.addAndGet(newEdges);
            logger.debug("cmd {} change count {}", command.getId(), count);
//            if (newEdges > 0) logger.info("cmd {} newEdges {}, change count {}", command.getId(), newEdges, count);
            long time = System.currentTimeMillis();
            long diff = (time - partitioningStartTime)/1000;
            logger.debug("time for starting repartitioning? time diff from previous repartition {}", diff);
            if (diff >= FEEDBACK_TIME_INTERVAL && this.objectGraph.getUpdatedEdgesBatch().size() > 0 && REPARTITIONING_ENABLED) {
                sendFeedback();
                partitioningStartTime = System.currentTimeMillis();
            }

            attempt = attemptToExecuteCommand(command);
            if (attempt instanceof Boolean) continue;

        }
    }

    private void attempToExecuteSSMRCommand(Command command) {

        //sending signal & data
        Set<Partition> dests = command.getDestinations();
        logger.debug("cmd {} ssmr was sent to {} ids {}", command.getId(), command.getDestinations());
        if (dests.size() <= 1) {
            logger.debug("cmd {} in SSMR mode, sent to one partition, local command", command.getId());
            attempToExecuteAppCommand(command);
            return;
        }

        Set<ObjId> objIds = Utils.extractObjectId(command);
        List<Partition> otherPartitions = new ArrayList<>(dests);
        otherPartitions.remove(replica.getPartition());
        Set<Integer> otherPartitionIds = new HashSet<>();
        otherPartitions.forEach(partition -> otherPartitionIds.add(partition.getId()));
        queuedCommand.put(command.getId(), new QueuedCommand(command, otherPartitionIds));
        logger.debug("cmd {} in SSMR mode, sent to {} partition, global command, going to send signal", command.getId(), otherPartitionIds);

        Map<ObjId, Message> diffs = new HashMap<>();
        objIds.forEach(objId -> {
            PRObjectNode node = this.objectGraph.getLocalObjects(objId);
            if (node != null) {
                diffs.put(objId, new Message(node.deepClone()));
                if (node.getPRObject() == null) {
                    System.out.println("ERROR: cmd " + command.getId());
                } else {
                    logger.trace("cmd {} SSMR collecting object {}", command.getId(), node);
                }
            }
        });

        Message signal = new Message(DynaStarMessageType.PARTITION_SIGNAL, command.getId(), replica.getPartitionId(), diffs);
        // TODO: reenable this. turn off for debugging
//        if (replica.isPartitionMulticaster(command)) {
        if (replica.isPartitionMulticaster()) {
            logger.debug("cmd {} Partition {}/{} multicast signal {} to partitions ", command.getId(), replicaId, getPartitionId(), command, otherPartitionIds);
            replica.reliableMulticast(otherPartitions, signal);
        }
        replica.waitForSSMRSignal(command, otherPartitionIds);
    }

    private boolean attemptToExecuteRepartitionCommand(Command command) {
        if (!command.isRepartitioning()) return false;
        int partitioningVersion = (int) command.getItem(0);
        if (partitioningVersion < this.objectGraph.getPartitioningVersion())
            return true; // receive old partitioning map
        Map<Integer, Set<ObjId>> processingMap = this.objectGraph.calculateRepartitioningMoves(partitioningVersion);
        logger.info("cmd {} ready to run repartitioning #{} - {}", command.getId(), partitioningVersion, processingMap);
        logger.debug("cmd {} going to move objects #{}", command.getId(), processingMap);
        for (Map.Entry<Integer, Set<ObjId>> entry : processingMap.entrySet()) {
            int destPartitionId = entry.getKey();
            if (destPartitionId != this.partitionId) {
                Set<ObjId> objIds = entry.getValue();
                Map<ObjId, Message> diffs = new HashMap<>();
                objIds.forEach(objId -> {
                    PRObjectNode node = this.objectGraph.getNode(objId);
                    if (node != null) {
                        diffs.put(objId, new Message(node.deepClone()));
                        if (node.getPRObject() == null) {
                            logger.error("cmd {} REPARTITIONING source, object content is null {}, full cmd {}", command.getId(), node, command.toFullString());
                            System.out.println("ERROR: cmd " + command.getId());
                        } else {
                            logger.trace("cmd {} REPARTITIONING source, move object {} to partition {}", command.getId(), node, destPartitionId);
                        }
                    } else {
                        logger.error("REPARTITIONING source, can't find object {} in this partition for command {}. Request retry", objId, command.getId());
                        System.out.println("REPARTITIONING source, can't find object " + objId + " in this partition for command " + command.getId() + ". Request retry");
                        System.exit(1);
                    }
                });
                ArrayList<Partition> partitions = new ArrayList<>();
                partitions.add(Partition.getPartition(destPartitionId));
                Message exchange = new Message(DynaStarMessageType.PROBJECT_BUNDLE_REPARTITIONING, command.getId(), diffs, getPartitionId());
                logger.debug("cmd {} REPARTITIONING sending {} objects to partition {}", command.getId(), diffs.keySet().size(), destPartitionId);
                lockObjects(objIds, command.getId());
                removeObjects(command, objIds);
                sendObject(partitions, command, exchange);
            }
        }

        Set<ObjId> expectedOjbIds = processingMap.get(this.partitionId);
        logger.debug("cmd {} REPARTITIONING source, start waiting for objects {} for repartitioning", command.getId(), expectedOjbIds.size());
        Set<PRObjectNode> objs = replica.waitForObjectExchanging(command, processingMap);
        logger.debug("cmd {} REPARTITIONING source, received enough objects, persit them", command.getId());
        replica.persitRepartitioningObjects(command.getId());
        logger.info("cmd {} applying new partitioning #{}", command.getId(), partitioningVersion);
        this.objectGraph.applyNewPartitioning(partitioningVersion);
        replica.notifyDone(command.getId());
        unlockObjects(command.getId());
        return true;
    }

    private void sendFeedback() {
//        executorService.execute(() -> {
        int changes = this.objectGraph.getUpdatedEdgesBatch().size();
//        Codec codec = new CodecUncompressedKryo();
//        Set newEges = (Set) codec.deepDuplicate(this.objectGraph.getUpdatedEdgesBatch());
        Message exchange = new Message(DynaStarMessageType.PARTITION_FEEDBACK, this.partitionId, this.objectGraph.getUpdatedEdgesBatch());
        ArrayList<Partition> partitions = new ArrayList<>(Partition.getOracleSet());
        if (replica.isPartitionMulticaster()) {
            logger.debug("Partition {}/{} multicast feedback {} to oracle ", replicaId, getPartitionId());
            replica.reliableMulticast(partitions, exchange);
        }
        this.objectGraph.clearUpdatedBatch();
        changesCount.addAndGet(-changes);
//        });
//        executorService.shutdown();
    }

    private int updateObjectGraphAsClientCommand(Command command) {
        Set<ObjId> objIds = Utils.extractObjectId(command);
        logger.debug("cmd {} updating edges between objects", command.getId(), objIds);
        return this.objectGraph.connect(objIds);
    }

    private Map<Integer, Set<ObjId>> gatherObjects(Command command, int destPartId) {
        Set<ObjId> objIds = Utils.extractObjectId(command);
        logger.debug("cmd {} gather objects: {} to partition {}", command.getId(), objIds, destPartId);
        int moveCount = 0;
        Map<Integer, Set<ObjId>> srcMap = new HashMap<>();
        Set<Partition> multicastDestPart = new HashSet<>();

        int i = objIds.size();
        for (ObjId objId : objIds) {
            PRObjectNode objLoc = this.objectGraph.getNode(objId);
            logger.trace("cmd {} analyzing objects: {} belongs to partition {}", command.getId(), objId, objLoc.getPartitionId());

            if (objLoc.getPartitionId() != destPartId) { // only moveObject objects not in same partition
                int srcPartId = objLoc.getPartitionId();
                if (srcMap.get(srcPartId) == null) srcMap.put(srcPartId, new HashSet<>());
                srcMap.get(srcPartId).add(objId);
                multicastDestPart.add(Partition.getPartition(srcPartId));
                moveCount++;
            }
        }
        if (moveCount == 0) { //only oracle has this case
            logger.debug("cmd {} no move needed, forward to partition destPartId", command.getId());
            command.setObjectMap(srcMap);
            forwardCommand(command, destPartId, false);
        } else {
            logger.debug("cmd {} perform {} moves to partition {}", command.getId(), moveCount, destPartId);
            multicastDestPart.add(Partition.getPartition(destPartId));

            Command moveCmd = new Command(destPartId, srcMap, command);
            moveCmd.setId(command.getId());
            moveCmd.setDestinations(multicastDestPart);
            moveCmd.setObjectMap(srcMap);
            moveCmd.setPartitioningVersion(this.objectGraph.getPartitioningVersion());

            //Set target partition for original command as the destination partition. This is for picking partition to reply
            Set tmp = new HashSet<Partition>();
            tmp.add(Partition.getPartition(destPartId));
            command.setDestinations(tmp);
            command.setPartitioningVersion(this.objectGraph.getPartitioningVersion());
            if (moveTPMonitor != null) moveTPMonitor.incrementCount();
            ClientMessage wrapper = new ClientMessage(DynaStarMessageType.GATHER, moveCmd);
            // TODO: reenable this. turn off for debugging
//            if (replica.isPartitionMulticaster(command)) {
            if (replica.isPartitionMulticaster()) {
                logger.debug("cmd {} Partition {}/{} multicast move command {} to partitions ", command.getId(), replicaId, getPartitionId(), moveCmd, multicastDestPart);
                replica.multicast(multicastDestPart, wrapper);
            }
        }
        return srcMap;
    }

    private boolean attempToExecuteAppCommand(Command command) {
        // need another check for existance of actual object
        boolean done = checkForObjectsAvailability(command, "RETRY");
        if (done) return done;
        command.rewind();
        Message reply = executeCommand(command);
        replica.sendReply(reply, command);
        return true;
    }

    private Object attemptToExecuteCommand(Command command) {
        Set<ObjId> objIds = Utils.extractObjectId(command);
        if (!shouldMove(command)) {
            logger.debug("cmd {} going to execute locally", command.getId());
            boolean done = attemptToExecuteGenericCommand(command);
            if (done) return done;
            command.rewind();
            return attempToExecuteAppCommand(command);
        }
        logger.debug("cmd {} CANNOT execute locally, trying to gather objects", command.getId());
        Map<Integer, Set<ObjId>> gatherObjectMap = gatherObjects(command, getPartitionWithMostObjects(objIds));
        return new Boolean(true);
    }

    private boolean attemptToExecuteGenericCommand(Command command) {
//        if (!isLocalCommand(command)) return false;
        command.rewind();

        MessageType msgType = (MessageType) command.getNext();
        if (!(msgType instanceof CommandType)) return false;
        CommandType cmdType = (CommandType) msgType;
        logger.debug("cmd {} processing local {} command", command.getId(), cmdType);
        Message reply = null;
        switch (cmdType) {
            case CREATE: {
                Set<PRObject> objs = (Set<PRObject>) command.getNext();
                Set<PRObjectNode> created = new HashSet<>();
                objs.forEach(prObject -> {
                    PRObject newObj = createObject(prObject);
                    logger.debug("cmd {} created object {}", command.getId(), newObj);
                    PRObjectNode node = new PRObjectNode(newObj, newObj.getId(), getPartitionId());
                    node.setOwnerPartitionId(this.partitionId);
                    this.objectGraph.addNode(node);
                    created.add(node);
                });
                reply = new Message(created);
                break;
            }
            case READ: {
                ObjId objId = (ObjId) command.getNext();
                PRObject obj = this.objectGraph.getPRObject(objId);
                reply = new Message(obj);
                break;
            }
            case READ_BATCH: {
                Set<ObjId> objIds = (HashSet<ObjId>) command.getNext();
                Set<PRObject> ret = new HashSet<>();
                objIds.forEach(objId -> ret.add(this.objectGraph.getPRObject(objId)));
                ;
                reply = new Message(ret);
                break;
            }
            case DELETE: {
                ObjId objId = (ObjId) command.getNext();
                this.objectGraph.removeNode(objId);
                reply = new Message("OK");
                break;
            }
        }
        if (reply != null) {
            replica.sendReply(reply, command);
            return true;
        }
        return false;
    }

    protected abstract PRObject createObject(PRObject prObject);

    public abstract Message executeCommand(Command command);


    public void updateObjectGraphAsOracleMap(Map<Integer, Set<ObjId>> map) {
        logger.debug("updating objectGraph as oracle map...{}", map);
        this.objectGraph.updateGraph(map);
    }

    protected boolean runDestinationTask(Command command, Map<Integer, Set<ObjId>> srcPartMap, boolean shouldUnlockObjects) {
        boolean done = attemptToExecuteGenericCommand(command);
        if (!done) {
            command.rewind();
            attempToExecuteAppCommand(command);
        }
        returnObjects(command, srcPartMap);
        if (shouldUnlockObjects) unlockObjects(command.getId());
        return true;
    }

    protected boolean runDestinationTask(QueuedCommand qCommand) {
        logger.debug("cmd {} destination, received enough objects", qCommand.command.getId());
        return runDestinationTask(qCommand.command, qCommand.srcMap, qCommand.shouldUnlockObjects);
    }

    protected boolean runSourceTask(QueuedCommand qCommand) {
        logger.debug("cmd {} source, received enough objects", qCommand.command.getId());
        unlockObjects(qCommand.command.getId());
        return true;
    }

    protected boolean runQueuedCommand(CmdId cmdId) {
        QueuedCommand cmd = queuedCommand.remove(cmdId);
        if (cmd.isSSMRCommand) return runSSMRTask(cmd);
        if (cmd.isDestination) return runDestinationTask(cmd);
        else return runSourceTask(cmd);
    }

    private boolean runSSMRTask(QueuedCommand qCommand) {
        logger.debug("cmd {} SSMR, received enough signal", qCommand.command.getId());
        attempToExecuteAppCommand(qCommand.command);
        return true;
    }

    public Object attemptToExecuteGatherCommand(Command commandWrapper) {
        if (!commandWrapper.isGathering()) return false;
        int destPartId = (int) commandWrapper.getNext();
        Map<Integer, Set<ObjId>> srcPartMap = (HashMap) commandWrapper.getNext();
        Command command = (Command) commandWrapper.getNext();
//        logger.debug("cmd {} gather command: send to {} as map {}", command.getId(), destPartId, srcPartMap);
        if (getPartitionId() == destPartId) { // if this is the destination partition
            logger.debug("cmd {} destination start waiting for objects", command.getId());
            lockObjects(Utils.extractObjectId(command), command.getId());
            queuedCommand.put(command.getId(), new QueuedCommand(command, srcPartMap, true, true));
            Set<PRObjectNode> objs = replica.waitForObjectLending(command, srcPartMap);
            return true;
        } else {
            Set<ObjId> objIds = srcPartMap.get(getPartitionId());
            logger.debug("cmd {} source, trying to send objects {} from this partition {} to partition {}", command.getId(), objIds, getPartitionId(), destPartId);
            Map<ObjId, Message> diffs = new HashMap<>();
            objIds.forEach(objId -> {
                PRObjectNode node = this.objectGraph.getNode(objId);
                if (node != null) {
                    diffs.put(objId, new Message(node.deepClone()));
//                    diffs.put(objId, new Message(node));
                    if (node.getPRObject() == null) {
//                        logger.error("cmd {} source, object content is null {}, full cmd {}", command.getId(), node, command.toFullString());
                        System.out.println("ERROR: cmd " + command.getId());
                    } else {
                        logger.trace("cmd {} source, move object {} to partition {}", command.getId(), node, destPartId);
                    }
                } else {
                    logger.error("source, can't find object {} in this partition for command {}. Request retry", objId, command.getId());
                    System.out.println("source, can't find object " + objId + " in this partition for command " + command.getId() + ". Request retry");
                    System.exit(1);
                }
            });
            ArrayList<Partition> partitions = new ArrayList<>();
            partitions.add(Partition.getPartition(destPartId));
            Message exchange = new Message(DynaStarMessageType.PROBJECT_BUNDLE_LENDING, command.getId(), diffs, getPartitionId());

            //lock not only object being exchanged, but also object of actual command
            lockObjects(objIds, command.getId());
            removeObjects(command, objIds);
            sendObject(partitions, command, exchange);
            queuedCommand.put(command.getId(), new QueuedCommand(command, srcPartMap, false));
            Set<PRObjectNode> objs = replica.waitForObjectReturning(command, objIds, destPartId);
            return true;
        }
    }

    private void sendObject(List<Partition> destination, Command command, Message objects) {
        // TODO: reenable this. turn off for debugging
//        if (!replica.isPartitionMulticaster(command)) {
        if (!replica.isPartitionMulticaster()) {
            logger.debug("cmd {} Partition {}/{} DOES NOT multicast object {} to partitions ", command.getId(), replicaId, getPartitionId(), command, destination);
            return;
        }
        logger.debug("cmd {} Partition {}/{} multicast object {} to partitions ", command.getId(), replicaId, getPartitionId(), command, destination);
        replica.reliableMulticast(destination, objects);
    }

    private void returnObjects(Command command, Map<Integer, Set<ObjId>> gatherObjectMap) {
//        if (!replica.isPartitionMulticaster(command))
//            return;
        logger.debug("cmd {} returning objects {}", command.getId(), gatherObjectMap);
        for (Map.Entry<Integer, Set<ObjId>> entry : gatherObjectMap.entrySet()) {
            if (entry.getKey() != getPartitionId()) {
                int partitionId = entry.getKey();
                Set<ObjId> objIds = entry.getValue();
                Map<ObjId, Message> diffs = new HashMap<>();
                objIds.forEach(objId -> {
                    PRObjectNode node = this.objectGraph.getNode(objId);
                    if (node != null && node.getPRObject() != null) {
                        diffs.put(objId, new Message(node.deepClone()));
//                    logger.debug("cmd {} source, return object {} to partition {}", command.getId(), node.getPRObject(), partitionId);
                    } else if (node != null) {
                        logger.error("cmd {} source, return object {} to partition {} but got null", command.getId(), node, partitionId);
                        System.out.println("return but get null" + command.getId() + " - " + node);
                    } else {
                        logger.error("source, can't find object {} in this partition for command {}", objId, command.getId());
                        System.out.println("source, can't find object " + objId + " in this partition for command " + command.getId() + ". Request retry");
                        System.exit(1);
                    }
                });
                Message exchange = new Message(DynaStarMessageType.PROBJECT_BUNDLE_RETURNING, command.getId(), diffs, getPartitionId());
                ArrayList<Partition> partitions = new ArrayList<>();
                partitions.add(Partition.getPartition(partitionId));
                removeObjects(command, objIds);
                // TODO: reenable this. turn off for debugging
//                if (replica.isPartitionMulticaster(command)) {
                if (replica.isPartitionMulticaster()) {
                    logger.debug("cmd {} Partition {}/{} multicast return object {} to partitions ", command.getId(), replicaId, getPartitionId(), command, partitions);
                    replica.reliableMulticast(partitions, exchange);
                }
            }
        }
    }


    public PRObject getObject(ObjId id) {
        return this.objectGraph.getPRObject(id);
    }

    public PRObjectNode indexObject(PRObject obj, int partitionId) {
        PRObjectNode node = new PRObjectNode(obj, obj.getId(), partitionId);
        this.objectGraph.addNode(node);
        return node;
    }

    public PRObjectNode indexObject(ObjId objId, int partitionId) {
        PRObjectNode node = new PRObjectNode(null, objId, partitionId);
        this.objectGraph.addNode(node);
        return node;
    }

    public void removeObject(ObjId id) {
        this.objectGraph.removeObject(id);
    }

    public void removeObjects(Command cmd, Set<ObjId> objIds) {
        logger.debug("cmd {} remove objects {}", cmd.getId(), objIds);
        objIds.forEach(objId -> removeObject(objId));
    }

    public void handlePartitionMap(Message m) {
        m.rewind();
        DynaStarMessageType type = (DynaStarMessageType) m.getNext();
        Map<Integer, Set<ObjId>> map = (Map<Integer, Set<ObjId>>) m.getNext();
        int partitioningId = (int) m.getNext();
        logger.debug("REPARTITIONING - saving new partitionning map #{}-{}...", partitioningId, map);
        this.objectGraph.savePartitionMap(map, partitioningId);
        Message signal = new Message(DynaStarMessageType.PARTITION_MAP_RECEIVED, this.partitionId, partitioningId);
        ArrayList<Partition> partitions = new ArrayList<>(Partition.getOracleSet());

        if (replica.isPartitionMulticaster()) {
            replica.reliableMulticast(partitions, signal);
            logger.debug("REPARTITIONING - inform the oracle {} with message {} ", partitions, signal);
        }
    }

    @Override
    public void handlePartitionCommand(Command command) {
        logger.debug("handling partitioning command {}", command);
        replica.queueNextAwaitingExecution(command);
    }


    class QueuedCommand {
        Command command;
        Map<Integer, Set<ObjId>> srcMap;
        Set<Integer> partitions; // waiting signal from these partition, for ssmr
        boolean isDestination;
        boolean shouldUnlockObjects = false;
        boolean isSSMRCommand = false;

        public QueuedCommand(Command command, Set<Integer> partitions) {
            this.setSSMRCommand(true);
            this.command = command;
            this.partitions = partitions;
        }

        public QueuedCommand(Command command, Map<Integer, Set<ObjId>> srcMap, boolean isDestination) {
            this.command = command;
            this.srcMap = srcMap;
            this.isDestination = isDestination;
        }

        public QueuedCommand(Command command, Map<Integer, Set<ObjId>> srcMap, boolean isDestination, boolean shouldUnlockObjects) {
            this.command = command;
            this.srcMap = srcMap;
            this.isDestination = isDestination;
            this.shouldUnlockObjects = shouldUnlockObjects;
        }

        public boolean isSSMRCommand() {
            return isSSMRCommand;
        }

        public void setSSMRCommand(boolean SSMRCommand) {
            isSSMRCommand = SSMRCommand;
        }
    }
}


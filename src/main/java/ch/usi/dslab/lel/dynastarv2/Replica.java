/*
 * Nest - A library for developing DSSMR-based services
 * Copyright (C) 2015, University of Lugano
 *
 *  This file is part of Nest.
 *
 *  Nest is free software; you can redistribute it and/or
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

/**
 * @author Eduardo Bezerra - eduardo.bezerra@usi.ch
 */

package ch.usi.dslab.lel.dynastarv2;

import ch.usi.dslab.bezerra.mcad.ClientMessage;
import ch.usi.dslab.bezerra.mcad.Group;
import ch.usi.dslab.bezerra.mcad.MulticastServer;
import ch.usi.dslab.bezerra.mcad.ReliableMulticastAgent;
import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.command.CmdId;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Replica {
    private static Logger logger;
    private static Replica thisReplica = null;
    protected BlockingQueue<Command> executionQueue;      // delivered command will be stored in this queue for execution
    boolean running = true;
    private StateMachine stateMachine;
    private Thread amcastDelivererThread;               // atomic multicast deliverer
    private Thread rmcastDelivererThread;               // reliable multicast deliverer
    private boolean isLoggingReplica;
    private ExchangeObjectsRepository receivedObjects;
    private MulticastServer multicastServer;            // multicast sender
    //    private MulticastClient multicastClient;
    private Partition partition = null;
    private ReentrantLock executionQueueLock = new ReentrantLock();

    public Replica() {
        this.executionQueue = new LinkedBlockingDeque<>();
        this.stateMachine = StateMachine.getMachine();
        this.receivedObjects = new ExchangeObjectsRepository(this.stateMachine);
    }

    /*
     * This reads a json file containing the system's configuration, i.e., the
     * partitions that are part of it. Notice that this is agnostic to how
     * partitions are constituted; the multicast agent is the one that should
     * manage the mapping of partitions to groups/rings/whatever implements the
     * partitions.
     *
     * { "partitions" : [ {"id" : 1} , {"id" : 2} ] }
     */
    public static Replica createReplica(MulticastServer mcServer, String configFile) {
        thisReplica = new Replica();
        thisReplica.multicastServer = mcServer;
//        thisReplica.multicastClient = multicastClient;
        Partition.loadPartitions(configFile);
        int partitionId = mcServer.getMulticastAgent().getLocalGroup().getId();
        thisReplica.partition = Partition.getPartition(partitionId);

//        Partition.getAllPartition().forEach(thisReplica::connectToPartition);

//        MDC.put("ROLE", thisReplica.partition.getType() + "-" + thisReplica.partition.getId() + "/" + thisReplica.multicastServer.getId());

        thisReplica.start();
        return thisReplica;
    }


    public MulticastServer getMulticastServer() {
        return multicastServer;
    }

    public void start() {
        amcastDelivererThread = new Thread(new AtomicMulticastDeliverer(this), "AtomicMulticastDeliverer");
        rmcastDelivererThread = new Thread(new ReliableMulticastDeliverer(this), "ReliableMulticastDeliverer");

        amcastDelivererThread.start();
        rmcastDelivererThread.start();
    }

    public int getPartitionId() {
        return this.partition.getId();
    }

    public Partition getPartition() {
        return this.partition;
    }

    public void waitForStateMachineToBeReady() {
        try {
            Semaphore smReady = stateMachine.getReadySemaphore();
            smReady.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Command takeNextAwaitingExecution(int timeoutMs) {
        try {
            executionQueueLock.lock();
            Command cmd = null;
            if (timeoutMs > 0) cmd = executionQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            else cmd = executionQueue.take();
            return cmd;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executionQueueLock.unlock();
        }
        return null;
    }

    public Command queueNextAwaitingExecution(Command c) {
        try {
            if (c.t_queue_time == 0) c.t_queue_time = System.currentTimeMillis();
            executionQueue.put(c);
            logger.debug("cmd {} queued for execution", c.getId());
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public void putBackAwaitingExecution(Command c) {
        try {
            ((LinkedBlockingDeque) executionQueue).putFirst(c);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void putBackAwaitingExecution(Map<Command, CmdId> waitingList, List<Command> waitingCommandQueue) {
        if (waitingCommandQueue.size() > 0) {
            logger.debug("putBackAwaitingExecution: queue is waiting {}, map {}", waitingCommandQueue, waitingList);
            executionQueueLock.lock();
            for (int j = waitingCommandQueue.size() - 1; j >= 0; j--) {
                Command cmd = waitingCommandQueue.remove(j);
                if (!executionQueue.contains(cmd)) {
//                    logger.debug("cmd {} added back to queue", cmd.getId());
                    putBackAwaitingExecution(cmd);
                    waitingList.remove(cmd);
                } else {
//                    logger.debug("putBackAwaitingExecution: {} is already in queue", cmd);
                }
            }
            executionQueueLock.unlock();
        } else {
//            logger.debug("putBackAwaitingExecution: queue is empty");
        }
    }

    public void sendReply(Message reply, Command command) {
        CmdId cmdId = command.getId();
        int clientId = cmdId.clientId;
        long requestId = cmdId.clientRequestSequence;

        DynaStarMessageType type;
        if (partition.isOracle()) {
            type = DynaStarMessageType.ORACLE_REPLY;
        } else {
            type = DynaStarMessageType.PARTITION_REPLY;
        }
        // decide if this server should send the reply to this client
        if (thisIsTheReplyingReplica(command)) {
            logger.debug("cmd {} replies with reply {}", command.getId(), reply);
            Message replyWrapper = new Message(type, requestId, partition.getId(), reply);
            command.t_partition_finish_excecute = System.currentTimeMillis();
            replyWrapper.copyTimelineStamps(command);
            multicastServer.sendReply(clientId, replyWrapper);
        } else {
            logger.debug("cmd {} finished but not the one to reply {}", command.getId(), reply);
        }
    }

    boolean thisIsTheReplyingReplica(Command command) {
//        logger.debug("cmd {} checking if this is the replying replica", command.getId());
        int clientId = command.getSourceClientId();
        if (stateMachine.isOracle()) {
//            logger.debug("cmd {} this orcle replica connected to client: {} ", command.getId(),multicastServer.isConnectedToClient(clientId));
            return multicastServer.isConnectedToClient(clientId);
        }
        Set<Partition> dests = command.getDestinations();
        List<Partition> partitions = new ArrayList<>();
        partitions.addAll(dests.stream().filter(partition -> partition.isPartition()).collect(Collectors.toList()));
//        logger.debug("cmd {} type {} is being sent to dest {}, calculated partition {}", command.getId(), command.getType(), dests, partitions);

        int senderPartitionIndex = clientId % partitions.size();
        Partition senderPartition = partitions.get(senderPartitionIndex);
        boolean thisIsTheReplyingPartition = partition == senderPartition;
        boolean clientIsConnectedToThisReplica = multicastServer.isConnectedToClient(clientId);
//        logger.debug("cmd {} this replica connected to client: {}", command.getId(), clientIsConnectedToThisReplica);
        return thisIsTheReplyingPartition && clientIsConnectedToThisReplica;
//        return clientIsConnectedToThisReplica;
    }

    private void handleObjectExchangeMessage(Message msg, String exchangeType) {
        msg.rewind();
        DynaStarMessageType type = (DynaStarMessageType) msg.getNext();
        CmdId cmdId = (CmdId) msg.getNext();
        Map<ObjId, Message> objs = (Map<ObjId, Message>) msg.getNext();
        int sourcePartitionId = (int) msg.getNext();
        logger.debug("cmd {} start storing {} objects from partition {}, isBorrowing={}", cmdId, objs.size(), sourcePartitionId, exchangeType);
        receivedObjects.storeObject(cmdId, objs, sourcePartitionId, exchangeType);
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public boolean isPartitionMulticaster(Command cmd) {
        List<Integer> partitionMembers = thisReplica.partition.getGroup().getMembers();
        int multicasterIndex = cmd.getId().clientId % partitionMembers.size();
        return this.multicastServer.getId() == partitionMembers.get(multicasterIndex);
    }

    public boolean isPartitionMulticaster() {
        return this.multicastServer.getId() == partition.getGroup().getMembers().get(0);
    }

    public void multicast(Set<Partition> dests, ClientMessage msg) {
        if (dests.isEmpty())
            return;
        // creating the array of destination groups to pass to the multicast agent
        Set<Group> groupdests = new HashSet<>(dests.size());
        groupdests.addAll(dests.stream().map(part -> part.getGroup()).collect(Collectors.toList()));

        msg.setSourcePartitionId(this.partition.getId());

        // send message through the multicast agent
        logger.debug("multicast {} to partitions {}", msg, dests);
        multicastServer.getMulticastAgent().multicast(groupdests, msg);
    }

    protected Set<PRObjectNode> waitForObjectLending(Command command, Map<Integer, Set<ObjId>> srcPartMap) {
        receivedObjects.queueCommand(command.getId(), srcPartMap, "BORROWING");
        return null;
    }

    protected Set<PRObjectNode> waitForObjectReturning(Command command, Set<ObjId> objIds, int destId) {
        Map<Integer, Set<ObjId>> map = new HashMap<>();
        map.put(destId, objIds);
        receivedObjects.queueCommand(command.getId(), map, "LENDING");
        return null;
    }

    protected Set<PRObjectNode> waitForObjectExchanging(Command command, Map<Integer, Set<ObjId>> srcPartMap) {
        receivedObjects.queueCommand(command.getId(), srcPartMap, "REPARTITIONING");
        Set<PRObjectNode> objs = receivedObjects.takeObj(command.getId());
        return objs;
    }

    protected Set<PRObjectNode> waitForSSMRSignal(Command command, Set<Integer> otherPartitions) {
        receivedObjects.queueSSMRSignal(command.getId(), otherPartitions);
        return null;
    }

    public void notifyDone(CmdId cmdId) {
        receivedObjects.markAsDone(cmdId);
    }

    public void reliableMulticast(List<Partition> dests, Message msg) {
        if (dests.isEmpty())
            return;

        // creating the array of destination groups to pass to the multicast agent
        ArrayList<Group> groupdests = new ArrayList<>(dests.size());
        int size = dests.size();
        for (int i = 0; i < size; i++) {
            groupdests.add(dests.get(i).getGroup());
        }

        // send message through the multicast agent
        ReliableMulticastAgent rma = (ReliableMulticastAgent) multicastServer.getMulticastAgent();
        logger.debug("reliable multicast message {} to destination {}", msg, dests);
        rma.reliableMulticast(groupdests, msg);
    }


    public void persitRepartitioningObjects(CmdId cmdId) {
        this.receivedObjects.persitObjects(cmdId, true);
    }

    private void handleSSMRSignal(Message msg) {
        msg.rewind();
        DynaStarMessageType type = (DynaStarMessageType) msg.getNext();
        CmdId cmdId = (CmdId) msg.getNext();
        int sourcePartitionId = (int) msg.getNext();
        Map<ObjId, Message> objs = (Map<ObjId, Message>) msg.getNext();
        logger.debug("cmd {} SSMR start storing {} objects from partition {}", cmdId, objs.size(), sourcePartitionId);
        receivedObjects.storeSignal(cmdId, sourcePartitionId, objs);
    }

    public static class ReliableMulticastDeliverer implements Runnable {
        Replica replica;
//        BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();

        public ReliableMulticastDeliverer(Replica replica) {
            this.replica = replica;
//            Thread deliverThread = new Thread(() -> {
//                while (true) {
//                    try {
//                        Message m = messageQueue.poll(1000, TimeUnit.MILLISECONDS);
//                        if (m == null) {
//                            logger.debug("polling r-message: no message received");
//                            continue;
//                        }
//                        DynaStarMessageType type = (DynaStarMessageType) m.getNext();
//                        switch (type) {
//                            case PROBJECT_BUNDLE_LENDING: {
//                                CmdId c = (CmdId) m.getNext();
//                                logger.debug("cmd {} r-delivered:: {}", c, m);
//                                replica.handleObjectExchangeMessage(m, "BORROWING");
//                                break;
//                            }
//                            case PROBJECT_BUNDLE_RETURNING: {
//                                CmdId c = (CmdId) m.getNext();
//                                logger.debug("cmd {} r-delivered:: {}", c, m);
//                                replica.handleObjectExchangeMessage(m, "LENDING");
//                                break;
//                            }
//                            case PROBJECT_BUNDLE_REPARTITIONING: {
//                                CmdId c = (CmdId) m.getNext();
//                                logger.debug("cmd {} r-delivered:: {}", c, m);
//                                replica.handleObjectExchangeMessage(m, "REPARTITIONING");
//                                break;
//                            }
//                            case PARTITION_FEEDBACK: {
//                                int partitionId = (int) m.getNext();
//                                Set<PRObjectGraph.Edge> edges = (Set<PRObjectGraph.Edge>) m.getNext();
//                                ((OracleStateMachine) replica.stateMachine).handleFeedback(partitionId, edges);
//                                break;
//                            }
//                            case ORACLE_PARTITION_MAP: {
//                                logger.debug("REPARTITIONING receving new partition map from the oracle {}", m);
//                                ((PartitionStateMachine) replica.stateMachine).handlePartitionMap(m);
//                                break;
//                            }
//                            case PARTITION_MAP_RECEIVED: {
//                                logger.debug("REPARTITIONING receving new reply of receiving map from partition {}", m);
//                                ((OracleStateMachine) replica.stateMachine).handleMapReceivingSignal(m);
//                                break;
//                            }
//                            case PARTITION_SIGNAL: {
//                                CmdId c = (CmdId) m.getNext();
//                                logger.debug("cmd {} SSMR r-delivered:: {}", c, m);
//                                replica.handleSSMRSignal(m);
//                            }
//                        }
//
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }, "ReliableMulticastDeliverThread-" + replica.stateMachine.replicaId);
//            deliverThread.start();
        }

        // ==============================================================
        // LocalReplica's thread for getting and sorting rmcast messages
        // ==============================================================
        @Override
        public void run() {
            ReliableMulticastAgent rma = (ReliableMulticastAgent) replica.multicastServer.getMulticastAgent();
            while (replica.running) {
                Message m = rma.reliableDeliver();
                if (m == null) {
                    logger.debug("polling r-message: no message received");
                    continue;
                }
                m.rewind();
                DynaStarMessageType type = (DynaStarMessageType) m.getNext();
                switch (type) {
                    case PROBJECT_BUNDLE_LENDING: {
                        CmdId c = (CmdId) m.getNext();
                        logger.debug("cmd {} r-delivered:: {}", c, m);
                        replica.handleObjectExchangeMessage(m, "BORROWING");
                        break;
                    }
                    case PROBJECT_BUNDLE_RETURNING: {
                        CmdId c = (CmdId) m.getNext();
                        logger.debug("cmd {} r-delivered:: {}", c, m);
                        replica.handleObjectExchangeMessage(m, "LENDING");
                        break;
                    }
                    case PROBJECT_BUNDLE_REPARTITIONING: {
                        CmdId c = (CmdId) m.getNext();
                        logger.debug("cmd {} r-delivered:: {}", c, m);
                        replica.handleObjectExchangeMessage(m, "REPARTITIONING");
                        break;
                    }
                    case PARTITION_FEEDBACK: {
                        int partitionId = (int) m.getNext();
                        List<PRObjectGraph.Edge> edges = (List<PRObjectGraph.Edge>) m.getNext();
                        logger.debug("feedback r-delivered from {} :: {}, edges {}", partitionId, m, edges);
                        ((OracleStateMachine) replica.stateMachine).handleFeedback(partitionId, edges);
                        break;
                    }
                    case ORACLE_PARTITION_MAP: {
                        logger.debug("REPARTITIONING receving new partition map from the oracle {}", m);
                        ((PartitionStateMachine) replica.stateMachine).handlePartitionMap(m);
                        break;
                    }
                    case PARTITION_MAP_RECEIVED: {
                        logger.debug("REPARTITIONING receving new reply of receiving map from partition {}", m);
                        ((OracleStateMachine) replica.stateMachine).handleMapReceivingSignal(m);
                        break;
                    }
                    case PARTITION_SIGNAL: {
                        CmdId c = (CmdId) m.getNext();
                        logger.debug("cmd {} SSMR r-delivered:: {}", c, m);
                        replica.handleSSMRSignal(m);
                    }
                }
//                try {
//                    messageQueue.put(m);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }
        }
    }

    private static class ExchangedObject {
        String exchangeType;
        Map<ObjId, PRObjectNode> objects = new HashMap<>();
        Map<ObjId, Integer> sourcePartitions = new HashMap<>();

        public ExchangedObject(String exchangeType) {
            this.exchangeType = exchangeType;
        }

        public void addObject(ObjId oid, PRObjectNode prObject, int sourcePartitionId) {
            objects.put(oid, prObject);
            sourcePartitions.put(oid, sourcePartitionId);
        }

        public int size() {
            return objects.keySet().size();
        }

        public Set<PRObjectNode> getObjects() {
            return new HashSet<>(this.objects.values());
        }
    }

    public static class ExchangeObjectsRepository {
        private Map<CmdId, ExchangedObject> allObjsReceived;
        private Map<CmdId, Set<Integer>> allSignalReceived;
        private Map<CmdId, Set<Integer>> expectedSignal;
        private Map<CmdId, Set<ObjId>> expectedObjs;
        private StateMachine stateMachine;


        public ExchangeObjectsRepository(StateMachine stateMachine) {
            allObjsReceived = new ConcurrentHashMap<>();
            expectedObjs = new ConcurrentHashMap<>();
            expectedSignal = new ConcurrentHashMap<>();
            allSignalReceived = new ConcurrentHashMap<>();
            this.stateMachine = stateMachine;
        }

        synchronized public void queueSSMRSignal(CmdId cmdId, Set<Integer> partitions) {
            expectedSignal.put(cmdId, partitions);
            attempToExecuteSSMRCommand(cmdId);
        }

        synchronized public void attempToExecuteSSMRCommand(CmdId cmdId) {
            Set<Integer> receivedSignal = allSignalReceived.get(cmdId);
            if (receivedSignal != null && expectedSignal.get(cmdId) != null && receivedSignal.size() == expectedSignal.get(cmdId).size()) {
                logger.debug("receivedSignal.size {} expectedSignalSize {}", receivedSignal.size(), expectedSignal.get(cmdId).size());
                logger.debug("cmd {} SSMR received enough signal, going to execute", cmdId);
                if (((PartitionStateMachine) stateMachine).getQueuedCommand().keySet().contains(cmdId)) {
                    ((PartitionStateMachine) stateMachine).executorService.execute(() -> {
                        logger.debug("cmd {} SSMR was queued, attempt to execute", cmdId);
                        ((PartitionStateMachine) stateMachine).runQueuedCommand(cmdId);
                        logger.debug("cmd {} finished execution", cmdId);
                        markAsDone(cmdId);
                    });
                }
            }
        }

        synchronized public void storeSignal(CmdId cmdId, int partitionId, Map<ObjId, Message> objDiffs) {
            logger.debug("cmd {} SSMR storing objects {}", cmdId, objDiffs);
            Set<Integer> partitionSignalsReceived = allSignalReceived.get(cmdId);
            if (partitionSignalsReceived == null) {
                partitionSignalsReceived = new HashSet<>();
                allSignalReceived.put(cmdId, partitionSignalsReceived);
            }
            partitionSignalsReceived.add(partitionId);
            //persitObjects
            logger.debug("cmd {} SSMR persiting objects {}", cmdId, objDiffs.keySet());
            for (Map.Entry<ObjId, Message> entry : objDiffs.entrySet()) {
                ObjId objId = entry.getKey();
                Message msg = entry.getValue();
                msg.rewind();
                PRObjectNode node = (PRObjectNode) msg.getNext();
                stateMachine.objectGraph.persitObject(node, false);
            }

            attempToExecuteSSMRCommand(cmdId);
        }

        synchronized public void queueCommand(CmdId cmdId, Map<Integer, Set<ObjId>> srcPartMap, String exchangeType) {
            final Set<ObjId> objIds = expectedObjs.get(cmdId) == null ? new HashSet<>() : expectedObjs.get(cmdId);
            expectedObjs.put(cmdId, objIds);

            if (exchangeType.equals("REPARTITIONING"))
                objIds.addAll(srcPartMap.get(thisReplica.getPartitionId()));
            else
                srcPartMap.keySet().stream().filter(partitionId -> partitionId != thisReplica.getPartitionId()).forEach(partitionId -> objIds.addAll(srcPartMap.get(partitionId)));
            ExchangedObject objs = allObjsReceived.get(cmdId);
            if (objs == null) {
                objs = new ExchangedObject(exchangeType);
                allObjsReceived.put(cmdId, objs);
            }
            if (exchangeType.equals("BORROWING"))
                logger.debug("cmd {} expect receving {} objects from all partition", cmdId, expectedObjs.get(cmdId).size());
            else if (exchangeType.equals("LENDING"))
                logger.debug("cmd {} expect receving replies back partition", cmdId);
            else if (exchangeType.equals("REPARTITIONING"))
                logger.debug("cmd {} REPARTITIONING expect receving {} objects from all partition", cmdId, expectedObjs.get(cmdId).size());

            attempToExecuteQueuedCommand(cmdId);
        }

        synchronized public boolean attempToExecuteQueuedCommand(CmdId cmdId) {
            ExchangedObject exchangedObjects = allObjsReceived.get(cmdId);
//            if (exchangedObjects != null && expectedObjs.get(cmdId) != null)
//                logger.debug("cmd {} exchangedObjects {}  size {} expectedObjs {} size {}", cmdId, exchangedObjects, exchangedObjects.size(), expectedObjs.get(cmdId), expectedObjs.get(cmdId).size());
            if (exchangedObjects != null && expectedObjs.get(cmdId) != null && exchangedObjects.size() == expectedObjs.get(cmdId).size()) {
                logger.debug("cmd {} receive enough objects, queue {}", cmdId, ((PartitionStateMachine) stateMachine).getQueuedCommand().keySet());
                if (((PartitionStateMachine) stateMachine).getQueuedCommand().keySet().contains(cmdId)) {
                    ((PartitionStateMachine) stateMachine).executorService.execute(() -> {
                        logger.debug("cmd {} was queued, attemp to execute", cmdId);
                        persitObjects(cmdId, false);
                        ((PartitionStateMachine) stateMachine).runQueuedCommand(cmdId);
                        logger.debug("cmd {} finished execution", cmdId);
                        markAsDone(cmdId);
                    });
                    return false;
                }
                return true;
            }
            return true;
        }

        synchronized public void persitObjects(CmdId cmdId, boolean setOwner) {
            logger.debug("cmd {} persiting objects", cmdId);
            allObjsReceived.get(cmdId).getObjects().forEach(prObjectNode -> {
                stateMachine.objectGraph.persitObject(prObjectNode, setOwner);
            });
        }

        synchronized public void storeObject(CmdId cmdId, Map<ObjId, Message> objDiffs, int partitionId, String exchangeType) {
            logger.debug("cmd {} storing objects {}", cmdId, objDiffs);
            ExchangedObject exchangedObjects = allObjsReceived.get(cmdId);
            if (exchangedObjects == null) {
                exchangedObjects = new ExchangedObject(exchangeType);
                allObjsReceived.put(cmdId, exchangedObjects);
            }
            for (Map.Entry<ObjId, Message> entry : objDiffs.entrySet()) {
                ObjId objId = entry.getKey();
                Message msg = entry.getValue();
                msg.rewind();
                PRObjectNode node = (PRObjectNode) msg.getNext();
                exchangedObjects.addObject(objId, node, partitionId);
            }
            // persit objects
//            if (!exchangeType.equals("REPARTITIONING")) persitObjects(cmdId);
            boolean shouldContinue = attempToExecuteQueuedCommand(cmdId);
            logger.debug("cmd {} not executed yet, going to notify cmd in wait", cmdId);
            if (shouldContinue) notifyAll();
        }

        synchronized public Set<PRObjectNode> takeObj(CmdId cmdId) {
            boolean ok;
            try {
                do {
                    ok = false;
                    ExchangedObject exchangedObjects = allObjsReceived.get(cmdId);
                    if (exchangedObjects == null || exchangedObjects.size() != expectedObjs.get(cmdId).size()) {
                        logger.debug("cmd {} exchangedObjects.size()={}, expectedObjs.get(cmdId).size()={}", cmdId, exchangedObjects.size(), expectedObjs.get(cmdId).size());
                        wait();
                        continue;
                    }
                    ok = true;
                    logger.debug("cmd {} receive enough objects", cmdId);
                } while (!ok);
            } catch (InterruptedException e) {
                System.err.println("!!! - something occurred while trying to get object from Map<Map<Object>>");
                e.printStackTrace();
            }
            return allObjsReceived.get(cmdId).getObjects();
        }

        synchronized public void markAsDone(CmdId cmdId) {
            allObjsReceived.remove(cmdId);
            expectedObjs.remove(cmdId);
            expectedSignal.remove(cmdId);
            allSignalReceived.remove(cmdId);
        }
    }

    private class AtomicMulticastDeliverer implements Runnable {
        Replica replica;
        Unbatcher unBatcher;

        public AtomicMulticastDeliverer(Replica replica) {
            this.replica = replica;
            this.unBatcher = new Unbatcher(replica);
        }

        @Override
        public void run() {
            replica.waitForStateMachineToBeReady();

            while (replica.running) {
                Message m = unBatcher.nextMessage();
                DynaStarMessageType type = (DynaStarMessageType) m.getNext();
                Command command = (Command) m.getNext();
                logger.debug("cmd {} a-delivered from {} :: {}", command.getId(), m.getSourcePartitionId(), command.toFullString());
                command.setInvolvedObjects(Utils.extractObjectId(command));
                command.rewind();
                switch (type) {
                    case CLIENT_COMMAND: {
                        if (command.getPartitioningVersion() != stateMachine.objectGraph.getPartitioningVersion() && !replica.partition.isOracle()) {
                            logger.debug("cmd {} has old version numnber {}, current is {}, request retry, checked in replica", command.getId(), command.getPartitioningVersion(), stateMachine.objectGraph.getPartitioningVersion());
                            replica.sendReply(new Message("INVALIDATE_CACHE", stateMachine.objectGraph.getPartitioningVersion()), command);
                            break;
                        }
                        // enable to test mcast
//                        replica.sendReply(new Message("OK"), command);
                        replica.queueNextAwaitingExecution(command);
                        break;
                    }
                    case CLIENT_SSMR_COMMAND: {
                        command.setSSMRExecution(true);
                        replica.queueNextAwaitingExecution(command);
                        break;
                    }
                    case FORWARD: {
                        if (replica.partition.isOracle()) {
                            ((OracleStateMachine) stateMachine).updateMemory(command);
                        } else {
                            ((PartitionStateMachine) stateMachine).updateObjectGraphAsOracleMap(command.getObjectMap());
                            replica.queueNextAwaitingExecution(command);
                        }
                        break;
                    }
                    case GATHER: {
                        ((PartitionStateMachine) stateMachine).updateObjectGraphAsOracleMap(command.getObjectMap());
                        command.setGathering(true);
                        replica.queueNextAwaitingExecution(command);
                        break;
                    }
                    case ORACLE_PARTITION: {
                        command.setRepartitioning(true);
                        command.setInvolvedObjects(stateMachine.objectGraph.getLocalObjects());
                        stateMachine.handlePartitionCommand(command);
                        break;
                    }
                }
            }
        }

        private class Unbatcher {
            Replica replica;
            Queue<Message> readyMessages;

            public Unbatcher(Replica lr) {
                replica = lr;
                readyMessages = new LinkedList<>();
            }

            public Message nextMessage() {
                if (!readyMessages.isEmpty()) {
                    return readyMessages.remove();
                } else {
                    Message m = replica.multicastServer.getMulticastAgent().deliverMessage();
                    if (m.isCorrupted()) { // TODO: need to investigate this
                        logger.error("Receiving corrupted message from client");
                        return nextMessage();
                    }
                    if (m.getItem(0) instanceof ClientMessage) {
                        m.rewind();
                        while (m.hasNext()) {
                            Message oneMessage = (Message) m.getNext();
                            oneMessage.unpackContents();
                            readyMessages.add(oneMessage);
                        }
                        return readyMessages.remove();
                    } else {
                        return m;
                    }
                }
            }
        }
    }
}

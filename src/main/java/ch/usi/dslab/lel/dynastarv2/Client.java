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
import ch.usi.dslab.bezerra.mcad.Group;
import ch.usi.dslab.bezerra.mcad.MulticastClient;
import ch.usi.dslab.bezerra.mcad.MulticastClientServerFactory;
import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.CommandType;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Created by longle on 17.07.17.
 */
public class Client implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    boolean running = true;
    private MulticastClient multicastClient;
    private Thread clientThread;
    private int clientId;
    private AtomicLong nextRequestId = new AtomicLong(0);
    private Map<Long, ClientCommand> outstandingCommands = new ConcurrentHashMap<>();
    private PRObjectGraph cache = PRObjectGraph.getInstance(-1);

    private EvenCallBack queryEventCallBack = null;
    private EvenCallBack retryCommandEventCallBack = null;

    public Client(int clientId, String systemConfigFile, String partitioningFile) {
        System.setErr(System.out);
        this.clientId = clientId;
        MDC.put("ROLE", "CLIENT-" + this.clientId);
        multicastClient = MulticastClientServerFactory.getClient(clientId, systemConfigFile);
        Partition.loadPartitions(partitioningFile);
        // for ridge: uncomment below
//        Partition.getAllPartition().forEach(this::connectToPartition);
        clientThread = new Thread(this, "DynsStarClientThread-" + clientId);
        clientThread.start();
    }

    private void connectToPartition(Partition partition) {
        List<Integer> partitionServers = partition.getGroup().getMembers();
        int contactServerIndex = clientId % partitionServers.size();
        int contactServer = partitionServers.get(contactServerIndex);
        multicastClient.connectToServer(contactServer);
    }

    @Override
    public void run() {
        while (running) {
            Message replyWrapper = multicastClient.deliverReply();

            //edge case of jmcast
            if (replyWrapper.isCorrupted()) {
                logger.error("received corrupted reply");
                continue;
            }


            DynaStarMessageType type = (DynaStarMessageType) replyWrapper.getItem(0);
            long reqId = (Long) replyWrapper.getItem(1);
            int partitionId = (Integer) replyWrapper.getItem(2);
            Message reply = (Message) replyWrapper.getItem(3);
            logger.debug("cmd ({}.{}) - partition {} reply type {} :: {}", this.clientId, reqId, partitionId, type, reply.toString());
            ClientCommand clientCommand = outstandingCommands.get(reqId);
            switch (type) {
                case PARTITION_REPLY: {
                    if (clientCommand == null) {
                        logger.debug("cmd ({}.{}) - Partition reply null {}", this.clientId, reqId, reply);
                        break;
                    }
                    logger.debug("cmd ({}.{}) - analyzing partition reply {} type {}", this.clientId, reqId, reply, type.toString());

                    if (reply.peekNext() != null && reply.peekNext().equals("INVALIDATE_CACHE")) {
                        logger.debug("cmd ({}.{}) - Invalidate objLocationCache {}", this.clientId, reqId, reply);
                        clientCommand.setRetry(true);
                        this.invalidateAndUpdateCache(reply);
                    } else if (shoudRetry(reply)) {
                        logger.debug("cmd ({}.{}) - Set retry with reply {}", this.clientId, reqId, reply);
                        clientCommand.setRetry(true);
                    }

                    if (clientCommand.isQuery) {
                        logger.debug("cmd ({}.{}) - add reply of partition {} :: {}", this.clientId, reqId, partitionId, reply);
                        clientCommand.addReply(partitionId, reply);
                    }

                    if (clientCommand.shouldRetry && clientCommand.isDeliverable) {
                        logger.debug("cmd ({}.{}) - going to retry with reply {}", this.clientId, reqId, reply);
                        outstandingCommands.remove(reqId);
                        retryCommand(clientCommand, reply);
                    } else if (clientCommand.isDeliverable) {
                        logger.debug("cmd ({}.{}) - going to deliver with reply {}", this.clientId, reqId, reply);
                        updateCacheFromPartitionReply(clientCommand.command, partitionId);
                        outstandingCommands.remove(reqId);
                        logger.debug("cmd ({}.{}) - DELIVERED with reply {}", this.clientId, reqId, reply);
                        clientCommand.deliver(reply);
                    }
                    break;
                }
                case ORACLE_REPLY: {

                    if (shouldDrop(reply)) {
                        logger.debug("cmd ({}.{}) - Oracle tell DROP command {}", this.clientId, reqId, clientCommand);
                        clientCommand.drop();
                    } else {
                        updateCacheFromOracleReply(reply);
                    }
                    break;
                }
            }
        }
    }

    private void retryCommand(ClientCommand clientCommand, Message reply) {
        Command command = clientCommand.command;
        command.rewind();
        logger.debug("cmd {} - retry count {}", command.getId(), clientCommand.retryCount);
        clientCommand.increaseRetryCount();
        if (clientCommand.shouldDrop) {
            clientCommand.drop();
            logger.error("dropping message {}", command);
            return;
        }
        if (retryCommandEventCallBack != null) retryCommandEventCallBack.callback(1);

        clientCommand.setRetry(false);
        executeCommand(clientCommand, true, false, reply);
    }

    private void updateCacheFromPartitionReply(Command command, int partitionId) {
//        command.rewind();
//        CommandType op = (CommandType) command.getNext();
//        logger.debug("updateCache - Try to update internal objLocationCache from command result");
//        Set<ObjId> objIds = Utils.extractObjectId(command);
//        objIds.forEach(objId -> {
//            PRObjectNode node = getObject(objId);
//            if (node == null) {
//                node = new PRObjectNode(objId, partitionId);
//                this.cache.addNode(node);
//            } else {
//                node.update(partitionId);
//            }
//            logger.debug("updateCache - internal objLocationCache updated {}", node);
//        });

//        if (op == CommandType.DELETE) {
//            ObjId oid = (ObjId) command.getNext();
//            this.cache.removeNode(oid);
//        }
    }

    private void invalidateAndUpdateCache(Message reply) {
        int partitioningId = (int) reply.getItem(1);
        if (partitioningId != this.cache.getPartitioningVersion()) {
            logger.debug("invalidateAndUpdateCache - up to version {}", partitioningId);
            this.cache.setPartitioningVersion(partitioningId);
            this.cache.invalidate();
        }
    }

    private void updateCacheFromOracleReply(Message reply) {
        Map<Integer, Set<ObjId>> objectMap = extractObjectMapFromOracleReply(reply);
        logger.debug("updateCacheFromQuery - try to update internal objLocationCache from query for {}", objectMap);
        for (Map.Entry<Integer, Set<ObjId>> entry : objectMap.entrySet()) {
            int partitionId = entry.getKey();
            Set<ObjId> objIds = entry.getValue();
            objIds.forEach(objId -> {
                PRObjectNode node = getObject(objId);
                if (node == null) {
                    node = new PRObjectNode(objId, partitionId);
                    this.cache.addNode(node);
                } else {
                    node.update(partitionId);
                }
            });
        }
    }

    private Map<Integer, Set<ObjId>> extractObjectMapFromOracleReply(Message reply) {
        reply.rewind();
        OracleStateMachine.Prophecy prophecy = (OracleStateMachine.Prophecy) reply.getNext();
        return prophecy.objectMap;
    }

    public CompletableFuture<Message> connect(ObjId id1, ObjId id2) {
        Command cmd = new Command(CommandType.CONNECT, id1, id2);
        return executeCommand(new ClientCommand(cmd), false, false, null);
    }

    public CompletableFuture<Message> create(Set<PRObject> objects, Set<ObjId> dependentObjs) {
        Command cmd = new Command(CommandType.CREATE, objects, dependentObjs);
        return executeCommand(new ClientCommand(cmd), false, true, null);
    }

    public CompletableFuture<Message> create(PRObject object, Set<ObjId> dependentObjs) {
        Set<PRObject> objects = new HashSet<>();
        objects.add(object);
        Command cmd = new Command(CommandType.CREATE, objects, dependentObjs);
        return executeCommand(new ClientCommand(cmd), false, true, null);
    }

    public CompletableFuture<Message> create(Set<PRObject> objects) {
        Command cmd = new Command(CommandType.CREATE, objects);
        return executeCommand(new ClientCommand(cmd), false, true, null);
    }

    public CompletableFuture<Message> create(PRObject object) {
        Set<PRObject> objects = new HashSet<>();
        objects.add(object);
        return create(objects);
    }

    public CompletableFuture<Message> read(ObjId id) {
        Command cmd = new Command(CommandType.READ, id);
        return executeCommand(new ClientCommand(cmd), false, false, null);
    }

    public CompletableFuture<Message> read(Set<ObjId> objIds) {
        Command cmd = new Command(CommandType.READ_BATCH, objIds);
        return executeCommand(new ClientCommand(cmd), false, false, null);
    }

    public CompletableFuture<Message> delete(ObjId id) {
        Command cmd = new Command(CommandType.DELETE, id);
        return executeCommand(new ClientCommand(cmd), false, true, null);
    }

    public CompletableFuture<Message> executeCommand(Command command) {
        command.setPartitioningVersion(this.cache.getPartitioningVersion());
        logger.debug("Initiate command {}", command);
        return executeCommand(new ClientCommand(command), false, false, null);
    }

    public CompletableFuture<Message> executeSSMRCommand(Command command) {
        long requestId = nextRequestId.incrementAndGet();
        ClientCommand cmd = new ClientCommand(command);
        cmd.command.setId(clientId, requestId);
        cmd.command.setSSMRExecution(true);
        logger.debug("Initiate SSMR command {}", cmd.command);
        outstandingCommands.put(requestId, cmd);
        Set<ObjId> objIds = Utils.extractObjectId(cmd.command);

        Set<Integer> destIds = objIds.stream().map(objId -> cache.getNode(objId).getPartitionId()).collect(Collectors.toSet());

        logger.debug("cmd {} SSMR, Destionation:{}", cmd.command.getId(), destIds);
        multicastCommand(cmd.command, destIds, false, true);
        return cmd;
    }

    // Main function for executing a command from client
    public CompletableFuture<Message> executeCommand(ClientCommand acmd, boolean forceQuery, boolean shouldIncludeOracle, Message retryReply) {
        logger.debug("cmd {} executeCommand - Executing command: {}", acmd.command.getId(), acmd.command);
        Set<ObjId> objsToAdd = new HashSet<>();
        // if this is a retry command, try to include missing objects based on reply of server
        if (retryReply != null && !acmd.isRangeCommand) {
            retryReply.rewind();
            if (retryReply.getItem(1) != null && retryReply.getItem(1) instanceof Set) {
                logger.debug("cmd {} executeCommand - update command as retry reply {}", acmd.command.getId(), retryReply.getItem(1));
                objsToAdd.addAll((Set) retryReply.getItem(1));
            }
        }
        // extract all objects touched by command;

        // check if client should send command to oracle: only in case there is some objects in not in cache.
        boolean shouldQuery = shouldQuery(acmd.command, forceQuery);
        Set<ObjId> objIds = Utils.extractObjectId(acmd.command);
        if (shouldQuery) {
            logger.debug("cmd {} executeCommand - send consult message to oracle and wait for response from destination for command {}", acmd.command.getId(), acmd.command);
            return executeQuery(acmd);
        } else {
            // send command directly to partitions
            int dest = getPartitionWithMostObjects(objIds);
            long requestId = nextRequestId.incrementAndGet();
            acmd.command.setId(clientId, requestId);
            acmd.setQuery(false);
            outstandingCommands.put(requestId, acmd);
            multicastCommand(acmd.command, new HashSet<>(singletonList(dest)), shouldIncludeOracle, false);
        }
        return acmd;
    }

    private void multicastCommand(Command command, Set<Integer> destinationIds, Boolean includeOracle, boolean forceSSMR) {
        if (null != destinationIds && destinationIds.size() != 0) {
            Set<Partition> destinations = new HashSet<>();
            Set<Group> destinationGroups = new HashSet<>();

            // also add oracle to the destination group
            if (includeOracle) {
                destinations.addAll(Partition.getOracleSet());
            }

            destinations.addAll(destinationIds.stream().map(Partition::getPartition).collect(Collectors.toList()));
            destinationGroups.addAll(destinations.stream().map(Partition::getGroup).collect(Collectors.toList()));


            command.setDestinations(destinations);
            command.t_client_send = System.currentTimeMillis();
            command.setPartitioningVersion(this.cache.getPartitioningVersion());

            logger.debug("cmd {} multicastCommand - Sending command {} to actual partition {}", command.getId(), command, command.getDestinations());

            ClientMessage commandMessage;
            if (!forceSSMR) {
                commandMessage = new ClientMessage(DynaStarMessageType.CLIENT_COMMAND, command);
            } else {
                commandMessage = new ClientMessage(DynaStarMessageType.CLIENT_SSMR_COMMAND, command);
            }
            multicastClient.multicast(destinationGroups, commandMessage);
        } else {
            // TODO: deal with empty answer;
            logger.debug("cmd {} multicastCommand - Can't locate object {} in oracle's memory. Created it yet?", command.getId(), command.getItem(1).toString());
        }
    }

    private CompletableFuture<Message> executeQuery(ClientCommand acmd) {
        long requestId = nextRequestId.incrementAndGet();
        acmd.reset();
        acmd.command.setId(clientId, requestId);
        acmd.setQuery(true);
        outstandingCommands.put(requestId, acmd);
        Set<Partition> destinations = new HashSet<>();
        Set<Group> destinationGroups = new HashSet<>();
        // also add oracle to the destination group
        destinations.addAll(Partition.getOracleSet());
        destinationGroups.addAll(destinations.stream().map(Partition::getGroup).collect(Collectors.toList()));
        acmd.command.setDestinations(destinations);
        acmd.command.t_client_send = System.currentTimeMillis();
        acmd.command.setPartitioningVersion(this.cache.getPartitioningVersion());
        logger.debug("cmd {} multicastCommand - consult command {} to oracle", acmd.command.getId(), acmd.command);
        if (queryEventCallBack != null) queryEventCallBack.callback(1);
        ClientMessage wrapper = new ClientMessage(DynaStarMessageType.CLIENT_COMMAND, acmd.command);
        multicastClient.multicast(destinationGroups, wrapper);
        return acmd;
    }

    private boolean shouldQuery(Command command, boolean forceQuery) {
        if (command.getReservedObjects() != null) return true;
        if (command.getItem(0) instanceof CommandType && command.getItem(0) == CommandType.CREATE) return true;
        Set<ObjId> objIds = Utils.extractObjectId(command);
        if (forceQuery) return true;
        for (ObjId i : objIds) {
            if (getObject(i) == null) {
                logger.debug("cmd {} don't have cache of object {}.", command.getId(), i);
                return true;
            }
        }
        return false;
    }

    synchronized public PRObjectNode getObject(ObjId objId) {
        return this.cache.getNode(objId);
    }

    private Map<ObjId, Integer> getObjectMapFromCache(Set<ObjId> objIds) {
        Map<ObjId, Integer> ret = new HashMap<>();
        for (ObjId objId : objIds) {
            PRObjectNode o = getObject(objId);
            if (o != null) ret.put(o.getId(), o.getPartitionId());
        }
        return ret;
    }

    private int getPartitionWithMostObjects(Set<ObjId> objIds) {
        Map<Integer, Integer> map = new HashMap<>();
        int partition = -1;
        int max = -1;
        for (ObjId objId : objIds) {
            if (getObject(objId) == null) continue;
            int partitionId = getObject(objId).getPartitionId();
            if (map.get(partitionId) == null) map.put(partitionId, 0);
            int count = map.get(partitionId) + 1;
            if (max < count) {
                max = count;
                partition = partitionId;
            }
            map.put(partitionId, count);
        }
        return partition;
    }

    private boolean shoudRetry(Message reply) {
        for (Object obj : reply.getContents()) {
            if (obj instanceof String && obj.equals("RETRY"))
                return true;
        }
        return false;
    }

    private boolean shouldDrop(Message reply) {
        for (Object obj : reply.getContents()) {
            if (obj instanceof String && obj.equals("DROP"))
                return true;
        }
        return false;
    }

    public int getId() {
        return clientId;
    }

    public PRObjectGraph getCache() {
        return cache;
    }

    public void registerMonitoringEvent(EvenCallBack queryEventCallBack, EvenCallBack retryCommandEventCallBack) {
        this.queryEventCallBack = queryEventCallBack;
        this.retryCommandEventCallBack = retryCommandEventCallBack;
    }

    private static class ClientCommand extends CompletableFuture<Message> {
        final static int RETRY_THRESHOLD = 2;
        Command command;
        Object orgObjId;
        boolean isRangeCommand = false;
        boolean isQuery = false;
        boolean shouldRetry = false;
        boolean shouldDrop = false;
        boolean isDeliverable = true;
        Message prophecyMessage = null;
        int destinationPartitionId;
        int repliedPartitionId;
        int retryCount = 0;

        public ClientCommand() {
        }

        public ClientCommand(Command command) {
            this.command = command;
            this.orgObjId = command.getItem(1);
        }

        public void setQuery(boolean isQuery) {
            this.isQuery = isQuery;
            this.isDeliverable = !isQuery;
            this.repliedPartitionId = -1;
        }

        public void setRetry(boolean shouldRetry) {
            this.shouldRetry = shouldRetry;
        }

        public void deliver(Message message) {
            this.complete(message);
        }

        public void drop() {
            this.complete(new Message("DROPPED"));
        }

        @Override
        public String toString() {
            String prefix = isQuery ? "[QUERY]" : "[EXECUTE]";
            return prefix + command.toString();
        }

        public void addReply(int partitionId, Message reply) {
            reply.next = 0;
            this.repliedPartitionId = partitionId;
            this.isDeliverable = true;
        }

        public ClientCommand reset() {
            this.prophecyMessage = null;
            this.destinationPartitionId = -1;
            this.repliedPartitionId = -1;
            this.isDeliverable = false;
            return this;
        }


        public void increaseRetryCount() {
            this.retryCount += 1;
            if (retryCount == RETRY_THRESHOLD) {
                this.shouldDrop = true;
            }
        }
    }
}

package ch.usi.dslab.lel.dynastarv2.partitioning;
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

import ch.usi.dslab.lel.dynastarv2.OracleStateMachine;
import ch.usi.dslab.lel.dynastarv2.Partition;
import ch.usi.dslab.lel.dynastarv2.command.CmdId;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.messages.DynaStarMessageType;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by longle on 25.08.17.
 */
public class MetisPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(OracleStateMachine.class);
    private OracleStateMachine oracleStateMachine;
    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Map<Integer, Map<Integer, Set<ObjId>>> METISPartitionMaps = new HashMap<>();
    private Map<Integer, Set<Integer>> signalsMap = new HashMap<>();

    public MetisPartitioner(OracleStateMachine oracleStateMachine) {
        this.oracleStateMachine = oracleStateMachine;
    }

    @Override
    public void repartition(PRObjectGraph objectGraph, int partitionsCount) {

        CompletableFuture task = CompletableFuture.supplyAsync(() -> {
            Future<Map<Integer, Set<ObjId>>> worker = executorService.submit(new PartitioningWorker(objectGraph, partitionsCount));
            try {
                Map<Integer, Set<ObjId>> ret = worker.get();
                logger.info("*** === Got metis result");
                return ret;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        });

        task.thenAccept(o -> {
            Map<Integer, Set<ObjId>> ret = (Map<Integer, Set<ObjId>>) o;
            int partitioningId = oracleStateMachine.objectGraph.getPartitioningVersion() + 1;
            if (ret == null) {
                logger.info("Partition unsuccessful #{}", partitioningId);
                System.out.println("Partition unsuccessful #{}" + partitioningId);
                return;
            }
            logger.info("Partitioning successfully #{} {}", partitioningId, ret);
            System.out.println("Partitioning successfully " + partitioningId);;
            objectGraph.savePartitionMap(ret, partitioningId);
            CmdId cmdid = new CmdId(this.oracleStateMachine.getReplicaId(), this.oracleStateMachine.getNextCommandId());
            Command partitionCommand = new Command(DynaStarMessageType.ORACLE_PARTITION_MAP, ret, partitioningId);
            partitionCommand.setId(cmdid);
            Set<Partition> dests = Partition.getPartitionList();
            partitionCommand.setDestinations(dests);

            logger.debug("going to send this message size {} to partitions {}, map size {}", partitionCommand.getPackSize(), dests, ret.size());

            if (this.oracleStateMachine.getReplica().isPartitionMulticaster()) {
                logger.debug("Oracle {}/{} multicast partitioning map", this.oracleStateMachine.getReplicaId(), this.oracleStateMachine.getPartitionId());
                this.oracleStateMachine.getReplica().reliableMulticast(new ArrayList<>(dests), partitionCommand);
            }
            return;
        });

    }

    @Override
    public void queueSignal(int partitionId, int partitioningId) {
        Set<Integer> signals = signalsMap.get(partitioningId);
        if (signals == null) {
            signals = new HashSet<>();
            signalsMap.put(partitioningId, signals);
        }
        signals.add(partitionId);
    }

    @Override
    public boolean isSignalsFullfilled(int partitioningId) {
        Set<Integer> signals = signalsMap.get(partitioningId);
        Assert.assertNotNull(signals);
        return signals.size() == Partition.getPartitionsCount();
    }


    class PartitioningWorker implements Callable<Map<Integer, Set<ObjId>>> {
        int partitionsCount;
        PRObjectGraph objectGraph;
        Map<ObjId, PRObjectNode> filteredObjectMap = new HashMap<>();
        Map<Integer, ObjId> mappingFromMetisToGraph= new HashMap<>();
        Map<ObjId, Integer> mappingFromGraphToMetis = new HashMap<>();

        public PartitioningWorker(PRObjectGraph objectGraph, int partitionsCount) {
            this.objectGraph = objectGraph;
            this.filteredObjectMap = objectGraph.getObjectMapping().entrySet().stream()
                    .filter(entry-> entry.getValue().isReserved()==false)
                    .collect(Collectors.toMap(p->p.getKey(), p->p.getValue()));
            this.partitionsCount = partitionsCount;
        }

        private String generateMetisInput(PRObjectGraph objectGraph) {
            List<String> lines = new ArrayList<>();
            logger.info("Current keys: {}", this.filteredObjectMap);
            lines.add(objectGraph.getEdges().size() + " " + filteredObjectMap.keySet().size());

            // since matrix contains [{1:1,2,3,4}, {10: 230,12,34}] => not contigous id
            // while metis input need it. So we need a map of {1:1} {2:10} for the matrix
            // metis expect id start with #1
            // initiate the map
            int j = 1;
            for (ObjId i : filteredObjectMap.keySet()) {
                mappingFromMetisToGraph.put(j, i);
                mappingFromGraphToMetis.put(i, j);
                j++;
            }


            for (Iterator<PRObjectGraph.Edge> i = objectGraph.getEdges().iterator(); i.hasNext(); ) {
                PRObjectGraph.Edge edge = i.next();
                StringBuffer line = new StringBuffer();
                for (PRObjectNode node : edge.nodes) {
                    line.append(mappingFromGraphToMetis.get(node.getId())+" ");
                }
                lines.add(line.toString());
            }

            String metisInputFileName = "hmetis.inp." + oracleStateMachine.getReplicaId() + "." + System.nanoTime();
            Path file = Paths.get(metisInputFileName);
            try {
                Files.write(file, lines, Charset.forName("UTF-8"));
                return metisInputFileName;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        private boolean processWithMetis(String fileName) {
            boolean success = false;
            try {
                // start up the command in child process
//                ProcessBuilder ps;
//                ProcessBuilder ps = new ProcessBuilder("/usr/local/bin/shmetis", fileName, String.valueOf(this.partitionsCount), "10");
                ProcessBuilder ps = new ProcessBuilder("hmetis", fileName, String.valueOf(this.partitionsCount));

                ps.redirectErrorStream(true);

                Process pr = ps.start();

                BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.indexOf("******") > -1) success = true;
                    logger.info(line);
                }
                pr.waitFor();
                in.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return success;
        }


        private Map<Integer, Set<ObjId>> parseMetisPartition(String fileName) {
            Map<Integer, Set<ObjId>> tmp = new HashMap<>();
            Partition.getPartitionList().forEach(partition -> tmp.put(partition.getId(), new HashSet<>()));
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                int metisUserId = 1;
                while ((line = br.readLine()) != null) {
                    ObjId objId = mappingFromMetisToGraph.get(metisUserId);
                    tmp.get(Integer.parseInt(line)).add(objId);
                    metisUserId++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("finish parsing file");
            return tmp;
        }

        @Override
        public Map<Integer, Set<ObjId>> call() throws Exception {
            logger.info("*** === Generating Metis input");
            String fileName = this.generateMetisInput(objectGraph);
            logger.info("*** === Processing With Metis");
            this.processWithMetis(fileName);
            logger.info("*** === Parsing Metis Output " + fileName);
            Map<Integer, Set<ObjId>> ret = this.parseMetisPartition(fileName + ".part." + this.partitionsCount);
            return ret;
        }
    }
}

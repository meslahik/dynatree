package ch.usi.dslab.lel.dynastarv2.probject;
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

import ch.usi.dslab.lel.dynastarv2.PartitionStateMachine;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by longle on 17.07.17.
 */
public class PRObjectGraph {
    private static PRObjectGraph instance = null;
    Logger logger = LoggerFactory.getLogger(PartitionStateMachine.class);
    private Map<ObjId, PRObjectNode> objectMapping = null;
    private Set<Edge> edges = Collections.newSetFromMap(new ConcurrentHashMap<Edge, Boolean>());
    private List<Edge> updatedEdgesBatch = Collections.synchronizedList(new ArrayList<Edge>()); // Collections.newSetFromMap(new ConcurrentHashMap<Edge, Boolean>()); //new HashSet<>();
    private int partitioningVersion = 0;
    private int partitionId;
    private Map<Integer, Map<Integer, Set<ObjId>>> partitioningMap;


    protected PRObjectGraph(int partitionId) {
        this.objectMapping = new ConcurrentHashMap<>();
//        this.edges =
        this.partitionId = partitionId;
        this.partitioningMap = new HashMap<>();
    }

    public static PRObjectGraph getInstance(int partitionId) {
        if (instance == null) {
            instance = new PRObjectGraph(partitionId);
        }
        return instance;
    }

    public Map<Integer, Set<ObjId>> getPartitioningMap(int partitionId) {
        return this.partitioningMap.get(partitionId);
    }

    public PRObject getPRObject(ObjId objId) {
        PRObjectNode node = objectMapping.get(objId);
        if (node != null && node.getPRObject() != null) return node.getPRObject();
        return null;
    }

    public PRObjectNode getNode(ObjId objId) {
        return objectMapping.get(objId);
    }

    public void addNode(PRObjectNode prObject) {
        objectMapping.put(prObject.getId(), prObject);
    }

    public void removeNode(PRObjectNode prObject) {
        removeNode(prObject.getId());
    }

    public void removeNode(ObjId objId) {
        objectMapping.remove(objId);
    }

    public int getPartitioningVersion() {
        return partitioningVersion;
    }

    public void setPartitioningVersion(int partitioningVersion) {
        this.partitioningVersion = partitioningVersion;
    }

    public void updatePRObject(ObjId objId, PRObjectNode node) {
        objectMapping.put(objId, node);
    }

    public void invalidate() {
        objectMapping = new ConcurrentHashMap<>();
    }

    private int getOptimizedDestinationWithMinimumMove(Map<ObjId, Integer> objectMap) {
        Map<Integer, Integer> calculation = new HashMap<>();
        Set<ObjId> objIds = objectMap.keySet();
        objIds.forEach(objId -> {
            int partitionId = getNode(objId).getPartitionId();
            if (calculation.get(partitionId) == null) calculation.put(partitionId, 1);
            else calculation.put(partitionId, calculation.get(partitionId) + 1);
        });
        final int[] dest = {-1};
        final int[] score = {-1};
        calculation.entrySet().forEach(entry -> {
            if (entry.getValue() > score[0]) {
                score[0] = entry.getValue();
                dest[0] = entry.getKey();
            }
        });
        return dest[0];
    }

    public Set<ObjId> getMissingObjects(Set<ObjId> objIds) {
        return objIds.stream().filter(objId -> getPRObject(objId) == null).collect(Collectors.toSet());
    }

    public Set<ObjId> getMissingNodes(Set<ObjId> objIds) {
        return objIds.stream().filter(objId -> getNode(objId) == null).collect(Collectors.toSet());
    }

    public void updateGraph(Map<Integer, Set<ObjId>> map) {
        map.entrySet().forEach(entry -> {
            entry.getValue().forEach(objId -> addOrUpdateNode(objId, entry.getKey()));
        });
    }

    private PRObjectNode addOrUpdateNode(ObjId objId, Integer partitionId) {
        PRObjectNode node = getNode(objId);
        if (node == null) {
            logger.debug("obj {} doesn't exist in memory, going to create one for partition {}", objId, partitionId);
            node = new PRObjectNode(objId, partitionId);
            addNode(node);
        }
        node.setPartitionId(partitionId);
        return node;
    }


    public int connect(Set<ObjId> objIds) {
        int count = 0;
        Set<PRObjectNode> nodes = objIds.stream().map(objId -> getNode(objId))
                .collect(Collectors.toSet());
        Edge e = new Edge(nodes);
        if (!edges.contains(e) && e.nodes.size() > 1) {
            edges.add(e);
            updatedEdgesBatch.add(e);
            count++;
            logger.debug("new edge is created {}", e);
        }
        return count;
    }

    public List<Edge> getUpdatedEdgesBatch() {
        return this.updatedEdgesBatch;
    }

    public void clearUpdatedBatch() {
        updatedEdgesBatch = Collections.synchronizedList(new ArrayList<>());
//        updatedEdgesBatch.clear();
    }

    public void persitObject(PRObjectNode obj, boolean setOwner) {
        PRObjectNode node = getNode(obj.getId());
        if (node == null) {
            // only when object is being moved from another partition in repartitioning process
            // thus that object would belong to this partition after all;
            node = addOrUpdateNode(obj.getId(), this.partitionId);
        }
        if (obj.getPRObject() == null) {
            logger.error("object {} have null content", obj.getId());
        }
        node.setPRObject(obj.getPRObject());
        if (setOwner) node.setOwnerPartitionId(this.partitionId);
    }

    public Map<ObjId, PRObjectNode> getObjectMapping() {
        return objectMapping;
    }

    public void setObjectMapping(Map<ObjId, PRObjectNode> objectMapping) {
        this.objectMapping = objectMapping;
    }

    public void removeObject(ObjId id) {
        PRObjectNode node = getNode(id);
        Assert.assertNotNull(node);
        node.setPRObject(null);
    }

    public int updateGraphEdges(List<Edge> newEdges) {
        int count = 0;
        for (Edge edge : newEdges) {
            if (!this.edges.contains(edge)) {
                this.edges.add(edge);
                count++;
            }
        }
        return count;
    }

    public Set<Edge> getEdges() {
        return edges;
    }

    public int getNodesCount() {
        return this.objectMapping.keySet().size();
    }

    public void savePartitionMap(Map<Integer, Set<ObjId>> map, int partitioningVersion) {
        this.partitioningMap.put(partitioningVersion, map);
    }

//    public void savePartitionMapWithList(Map<Integer, List<ObjId>> map, int partitioningVersion) {
//        Map<Integer, Set<ObjId>> tmp = new HashMap<>();
//        for (Map.Entry<Integer, List<ObjId>> entry : map.entrySet()) {
//            int pid = entry.getKey();
//            List<ObjId> objIds = entry.getValue();
//            tmp.put(pid, new HashSet<>(objIds));
//        }
//        this.partitioningMap.put(partitioningVersion, tmp);
//    }

    public void applyNewPartitioning(int partitioningVersion) {
        this.updateGraph(this.partitioningMap.get(partitioningVersion));
        this.partitioningVersion = partitioningVersion;
        if (this.partitioningMap.get(partitioningVersion).get(this.partitionId) != null) { // not run the check on oracle
            for (ObjId objId : this.partitioningMap.get(partitioningVersion).get(this.partitionId)) {
                if (this.getPRObject(objId) == null) {
                    logger.error("REPARTITIONING can't find object {} in this partition", objId);
                }
            }
        }
    }

//    public Map<Integer, Set<ObjId>> calculateRepartitioningMoves(int partitioningVersion) {
//        Map<Integer, Set<ObjId>> ret = new HashMap<>();
//        Map<Integer, Set<ObjId>> nextPartitionMap = this.partitioningMap.get(partitioningVersion);
//        for (Map.Entry<Integer, Set<ObjId>> entry : nextPartitionMap.entrySet()) {
//            int targetPartitionId = entry.getKey();
//            Set<ObjId> targetObjs = entry.getValue();
//            Set<ObjId> objToProcess = new HashSet<>();
//            ret.put(targetPartitionId, objToProcess);
//            for (ObjId objId : targetObjs) {
//                if (this.partitionId == targetPartitionId) {
//                    if (this.getPRObject(objId) == null) {
//                        objToProcess.add(objId);
//                    }
//                    continue;
//                }
//                if (this.getPRObject(objId) != null) {
//                    objToProcess.add(objId);
//                }
//            }
//        }
//        return ret;
//    }

    public Map<Integer, Set<ObjId>> calculateRepartitioningMoves(int partitioningVersion) {
        Map<Integer, Set<ObjId>> ret = new HashMap<>();
        logger.debug("getting partitioning #{}", partitioningVersion);
        Map<Integer, Set<ObjId>> nextPartitionMap = this.partitioningMap.get(partitioningVersion);
        logger.debug("partitioning #{} - {}", partitioningVersion, nextPartitionMap);
        try {
            for (Map.Entry<Integer, Set<ObjId>> entry : nextPartitionMap.entrySet()) {

                int targetPartitionId = entry.getKey();
                Set<ObjId> targetObjs = entry.getValue();
                logger.debug("p#{} checking move for partition {}-{}", this.partitionId, targetPartitionId, targetObjs);
                Set<ObjId> objToProcess = new HashSet<>();
                ret.put(targetPartitionId, objToProcess);
                for (ObjId objId : targetObjs) {
                    logger.debug("p#{} objId={} targetPartitionId={}", this.partitionId, objId, targetPartitionId);
                    if (this.partitionId == targetPartitionId) {
                        logger.debug("p#{} expecting obj {} to partition {} ", this.partitionId, objId, targetPartitionId);
                        if (this.getPRObject(objId)==null){
//                        if (this.getNode(objId).getOwnerPartitionId() != this.partitionId) {
                            objToProcess.add(objId);
                        }
                        continue;
                    }
                    if (this.getPRObject(objId)!=null){
//                    if (this.getNode(objId).getOwnerPartitionId() == this.partitionId) {
                        logger.debug("p#{} moving obj {} to partition {} ", this.partitionId, objId, targetPartitionId);
                        objToProcess.add(objId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("exception - {}", e);
            e.printStackTrace();
            throw e;
        }
        return ret;
    }

    public Set<ObjId> getLocalObjects() {
        Set<ObjId> ret = new HashSet<>();
        for (Map.Entry<ObjId, PRObjectNode> entry : this.objectMapping.entrySet()) {
            ObjId objId = entry.getKey();
            if (entry.getValue().getPartitionId() == this.partitionId) ret.add(objId);
        }
        return ret;

    }

    public PRObjectNode getLocalObjects(ObjId objId) {
        if (this.getNode(objId) != null && this.getPRObject(objId) != null && this.getNode(objId).getOwnerPartitionId() == this.partitionId) {
            return this.getNode(objId);
        }
        return null;
    }

    public static class Edge implements Serializable {
//        public Set<PRObjectNode> nodes = new HashSet<>();
        public Set<PRObjectNode> nodes = Collections.newSetFromMap(new ConcurrentHashMap<PRObjectNode, Boolean>());

        public Edge() {

        }

        public Edge(Set<PRObjectNode> nodes) {
            this.nodes = nodes;
        }

        public void addNode(PRObjectNode node) {
            this.nodes.add(node);
        }

        public void addNode(Set<PRObjectNode> nodes) {
            this.nodes.addAll(nodes);
        }

        @Override
        public boolean equals(Object obj) {
            return nodes.equals(((Edge) obj).nodes);
        }

        @Override
        public int hashCode() {
            return nodes.hashCode();
        }

        @Override
        public String toString() {
            return "[Edges-" + this.nodes.size() + "]/["+this.nodes+"]";
        }
    }
}

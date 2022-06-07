/*

 Eyrie - A library for developing SSMR-based services
 Copyright (C) 2014, University of Lugano
 
 This file is part of Eyrie.
 
 Eyrie is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 
*/

/**
 * @author Eduardo Bezerra - eduardo.bezerra@usi.ch
 */

package ch.usi.dslab.lel.dynastarv2;

import ch.usi.dslab.bezerra.mcad.Group;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Partition implements Comparable<Partition> {
    private static Map<Integer, Partition> partitionList = new HashMap<>();
    private int id;
    private Group pgroup;
    private String type;
    static Set<Partition> oracleSet;
    static Set<Partition> partitionSet;
    static Set<Partition> allPartitionSet;

    public Partition() {
    }

    public Partition(int id, String type) {
        this.id = id;
        this.type = type;
        pgroup = Group.getOrCreateGroup(id); // assuming partition's and group's ids will match
        assert (pgroup != null);
        partitionList.put(id, this);
    }

    public static Partition getPartition(int id) {
        return partitionList.get(id);
    }

    public static Set<Partition> getPartitions(List<Integer> ids) {
        Set<Partition> ret = new HashSet<>();
        ids.stream().forEach(id -> ret.add(Partition.getPartition(id)));
        return ret;
    }

    public static void loadPartitions(String configFile) {
        try {
            JSONParser parser = new JSONParser();
            Object obj;

            obj = parser.parse(new FileReader(configFile));

            JSONObject partitions = (JSONObject) obj;

            JSONArray partitionArray = (JSONArray) partitions.get("partitions");

            for (Object aPartitionArray : partitionArray) {
                JSONObject partition = (JSONObject) aPartitionArray;
                long partition_id = (Long) partition.get("id");
                String partitionType = (String) partition.get("type");
                new Partition((int) partition_id, partitionType);
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        List<Partition> partList = new ArrayList<>();
        partList.addAll(partitionList.values().stream().filter(p -> p.isOracle()).collect(Collectors.toList()));
        Collections.sort(partList, (o1, o2) -> o1.getId() - o2.getId());
        oracleSet = new HashSet<>(partList);

        partList = new ArrayList<>();
        partList.addAll(partitionList.values().stream().filter(p -> p.isPartition()).collect(Collectors.toList()));
        Collections.sort(partList, (o1, o2) -> o1.getId() - o2.getId());
        partitionSet = new HashSet<>(partList);

        partList = new ArrayList<>(partitionList.values());
        Collections.sort(partList, (o1, o2) -> o1.getId() - o2.getId());
        allPartitionSet = new HashSet<>(partList);
    }

    public static Set<Partition> getAllPartition() {
        return allPartitionSet;
    }

    public static Set<Partition> getPartitionList() {
        return partitionSet;
    }

    public static Set<Partition> getOracleSet() {
        return oracleSet;
    }

    public static Set<Integer> getOracleIdSet() {
        return oracleSet.stream().map(partition -> partition.getId()).collect(Collectors.toSet());
    }


    public static int getPartitionsCount() {
        return getPartitionList().size();
    }

    public int getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public boolean isOracle() {
        return this.type.equals("ORACLE");
    }

    public boolean isPartition() {
        return this.type.equals("PARTITION");
    }

    public Group getGroup() {
        return pgroup;
    }

    @Override
    public boolean equals(Object Oother) {
        Partition other = (Partition) Oother;
        return this.id == other.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public int compareTo(Partition other) {
        return this.pgroup.getId() - other.pgroup.getId();
    }

    @Override
    public String toString() {
        return "P_"+ id;
    }

}

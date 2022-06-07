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

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by longle on 29.08.17.
 */
public class GraphRepartitioningCalculationTest {
    {

    }

    @Test
    public void testCalculateRepartitioningMoves() {
//        PRObjectGraph graph = PRObjectGraph.getInstance(1);
//        graph.addNode(new PRObjectNode(new SampleObject(1), new ObjId(1), 1));
//        graph.addNode(new PRObjectNode(new SampleObject(5), new ObjId(5), 1));
//        graph.addNode(new PRObjectNode(new SampleObject(9), new ObjId(9), 1));
//        graph.addNode(new PRObjectNode(new ObjId(2), 2));
//        graph.addNode(new PRObjectNode(new ObjId(6), 2));
//        graph.addNode(new PRObjectNode(new ObjId(10), 2));
//        graph.addNode(new PRObjectNode(new ObjId(3), 3));
//        graph.addNode(new PRObjectNode(new ObjId(7), 3));
//        graph.addNode(new PRObjectNode(new ObjId(11), 3));
//        graph.addNode(new PRObjectNode(new ObjId(4), 4));
//        graph.addNode(new PRObjectNode(new ObjId(8), 4));
//        graph.addNode(new PRObjectNode(new ObjId(12), 4));


//        PRObjectGraph graph = PRObjectGraph.getInstance(2);
//        graph.addNode(new PRObjectNode(new ObjId(1), 1));
//        graph.addNode(new PRObjectNode(new ObjId(5), 1));
//        graph.addNode(new PRObjectNode(new ObjId(9), 1));
//        graph.addNode(new PRObjectNode(new SampleObject(2), new ObjId(2), 2));
//        graph.addNode(new PRObjectNode(new SampleObject(6), new ObjId(6), 2));
//        graph.addNode(new PRObjectNode(new SampleObject(10), new ObjId(10), 2));
//        graph.addNode(new PRObjectNode(new ObjId(3), 3));
//        graph.addNode(new PRObjectNode(new ObjId(7), 3));
//        graph.addNode(new PRObjectNode(new ObjId(11), 3));
//        graph.addNode(new PRObjectNode(new ObjId(4), 4));
//        graph.addNode(new PRObjectNode(new ObjId(8), 4));
//        graph.addNode(new PRObjectNode(new ObjId(12), 4));


        PRObjectGraph graph = PRObjectGraph.getInstance(3);
        graph.addNode(new PRObjectNode(new ObjId(1), 1));
        graph.addNode(new PRObjectNode(new ObjId(5), 1));
        graph.addNode(new PRObjectNode(new ObjId(9), 1));
        graph.addNode(new PRObjectNode(new ObjId(2), 2));
        graph.addNode(new PRObjectNode(new ObjId(6), 2));
        graph.addNode(new PRObjectNode(new ObjId(10), 2));
        graph.addNode(new PRObjectNode(new SampleObject(3), new ObjId(3), 3));
        graph.addNode(new PRObjectNode(new SampleObject(7), new ObjId(7), 3));
        graph.addNode(new PRObjectNode(new SampleObject(11), new ObjId(11), 3));
        graph.addNode(new PRObjectNode(new ObjId(4), 4));
        graph.addNode(new PRObjectNode(new ObjId(8), 4));
        graph.addNode(new PRObjectNode(new ObjId(12), 4));


        Map<Integer, Set<ObjId>> next = new HashMap<>();
        Set<ObjId> p1 = new HashSet<>();
        p1.add(new ObjId(1));
        p1.add(new ObjId(2));
        p1.add(new ObjId(3));
        next.put(1, p1);

        Set<ObjId> p2 = new HashSet<>();
        p2.add(new ObjId(4));
        p2.add(new ObjId(5));
        p2.add(new ObjId(6));
        next.put(2, p2);

        Set<ObjId> p3 = new HashSet<>();
        p3.add(new ObjId(7));
        p3.add(new ObjId(8));
        p3.add(new ObjId(9));
        next.put(3, p3);

        Set<ObjId> p4 = new HashSet<>();
        p4.add(new ObjId(10));
        p4.add(new ObjId(11));
        p4.add(new ObjId(12));
        next.put(4, p4);

        graph.savePartitionMap(next, 1);


        Map<Integer, Set<ObjId>> ret = graph.calculateRepartitioningMoves(1);
        System.out.print(ret);
    }

    class SampleObject extends PRObject {
        int value;

        public SampleObject(int value) {
            this.value = value;
        }

        @Override
        public void updateFromDiff(Message objectDiff) {

        }

        @Override
        public Message getSuperDiff() {
            return null;
        }
    }
}

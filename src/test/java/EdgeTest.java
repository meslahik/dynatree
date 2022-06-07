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

import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.junit.Test;

/**
 * Created by longle on 02.08.17.
 */
public class EdgeTest {
    @Test
    public void testEdgeEquality() {
        PRObjectGraph.Edge e1 = new PRObjectGraph.Edge();
        e1.addNode(new PRObjectNode(new ObjId(1),1));
        e1.addNode(new PRObjectNode(new ObjId(2),1));

        PRObjectGraph.Edge e2 = new PRObjectGraph.Edge();
        e2.addNode(new PRObjectNode(new ObjId(1),1));
        e2.addNode(new PRObjectNode(new ObjId(2),1));

        System.out.println(e1.hashCode());
        System.out.println(e2.hashCode());
        System.out.println(e1.equals(e2));
    }
}

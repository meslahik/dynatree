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

import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;

import java.util.*;

/**
 * Created by longle on 17.07.17.
 */
public class Utils {

    static Random randomGenerator = new Random(System.nanoTime());

    public static Set<ObjId> extractObjectId(Command cmd) {
        if (cmd.getInvolvedObjects() != null && cmd.getInvolvedObjects().size() > 0) return cmd.getInvolvedObjects();
        if (StateMachine.isCreateWithGather(cmd)) {
            cmd.setInvolvedObjects((Set<ObjId>) cmd.getItem(2));
            return (Set<ObjId>) cmd.getItem(2);
        }
        cmd.rewind();
        Set<ObjId> objIds = new HashSet<>();
        while (cmd.hasNext()) {
            Object test = cmd.getNext();
            if (test instanceof ObjId) {
                objIds.add((ObjId) test);
            } else if (test instanceof List) {
                for (Object objId : (List) test) {
                    if (objId instanceof ObjId) {
                        objIds.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Set) {
                for (Object objId : (Set) test) {
                    if (objId instanceof ObjId) {
                        objIds.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Map) {
                for (Object objId : ((Map) test).keySet()) {
                    if (objId instanceof ObjId) {
                        objIds.add((ObjId) objId);
                    }
                }
                for (Object objId : ((Map) test).values()) {
                    if (objId instanceof ObjId) {
                        objIds.add((ObjId) objId);
                    } else if (objId instanceof Set) {
                        for (Object o : (Set) objId) {
                            if (o instanceof ObjId) {
                                objIds.add((ObjId) o);
                            }
                        }
                    }
                }
            } else if (test instanceof Command) {
                ((Command) test).rewind();
                objIds.addAll(extractObjectId((Command) test));
            }
        }
        Set<ObjId> tmp = extractRelatedObjects(cmd);
        if (tmp != null) objIds.addAll(tmp);
        cmd.setInvolvedObjects(objIds);
        return objIds;
    }

    public static Set<ObjId> extractRelatedObjects(Command cmd) {
        return null;
    }

    public static int getObjectPlacement(ObjId oid) {
        return oid.value % Partition.getPartitionsCount() + 1;
    }

    public static int getObjectPlacement(Set<ObjId> oids) {
        for (ObjId objId : oids) {
//            if (objId != null) return objId.value % Partition.getPartitionsCount() + 1;
            // JMCAST: use above for ridge, below for jmcast
            if (objId != null) return objId.value % Partition.getPartitionsCount();
        }
//        return randomGenerator.nextInt(Partition.getPartitionsCount()) + 1;
        // JMCAST: use above for ridge, below for jmcast
        return randomGenerator.nextInt(Partition.getPartitionsCount());
    }
}

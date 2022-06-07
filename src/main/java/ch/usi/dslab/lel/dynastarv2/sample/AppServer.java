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

package ch.usi.dslab.lel.dynastarv2.sample;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.PartitionStateMachine;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.apache.log4j.Logger;

import java.util.Set;

public class AppServer extends PartitionStateMachine {
    static final boolean is64bit = true;
    private static final Logger logger = Logger.getLogger(AppServer.class);

    public AppServer(int serverId, String systemConfig, String partitionsConfig) {
        super(serverId, systemConfig, partitionsConfig);
    }

    public static void main(String[] args) {
        System.out.println("Starting server...");
        if (args.length != 3) {
            System.err.println("usage: serverId systemConfigFile partitioningFile");
            System.exit(1);
        }

        int serverId = Integer.parseInt(args[0]);

        String systemConfigFile = args[1];

        String partitionsConfigFile = args[2];

        AppServer server = new AppServer(serverId, systemConfigFile, partitionsConfigFile);

        server.runStateMachine();
    }

    @Override
    protected PRObject createObject(PRObject prObject) {
        return (AppObject) prObject;
    }

    @Override
    public Message executeCommand(Command cmd) {
        cmd.rewind();
        // Command format : | byte op | ObjId o1 | int value |
        System.out.println("AppServer: Processing command " + cmd.getId().toString() + " - " + cmd);
        AppCommand op = (AppCommand) cmd.getNext();
        AppObject objs[] = new AppObject[cmd.count() - 1]; // minus OP
        int len = 0;
        while (cmd.hasNext()) {
            ObjId oid = (ObjId) cmd.getNext();
            AppObject obj = (AppObject) objectGraph.getPRObject(oid);
            if (obj == null) {
                System.out.println("ERROR: " + this.partitionId + " - " + oid + " is null");
                continue;
            }
            objs[len++] = obj;
        }

        switch (op) {
            case ADD: {
                for (int i = 1; i < len; i++) {
                    objs[0].add(objs[i].getValue());
                }
                break;
            }
            case SUB: {
                for (int i = 1; i < len; i++) {
                    objs[0].sub(objs[i].getValue());
                }
                break;
            }
            case MUL: {
                for (int i = 1; i < len; i++) {
                    objs[0].mul(objs[i].getValue());
                }
                break;
            }
            case DIV: {
                for (int i = 1; i < len; i++) {
                    objs[0].div(objs[i].getValue());
                }
                break;
            }
            default:
                System.out.println("Command is not defined");
                break;
        }

        if (cmd.getReservedObjects() != null) {
            Set<PRObject> reservedObjects = cmd.getReservedObjects();
            for (PRObject object : reservedObjects) {
                System.out.println("Creating reserved objects " + object);
                PRObject newObj = this.createObject(object);
                PRObjectNode node = new PRObjectNode(newObj, newObj.getId(), getPartitionId());
                this.objectGraph.addNode(node);
            }
        }
        System.out.println("Object " + objs[0].getId() + " = " + objs[0].getValue());
        return new Message(objs[0]);
    }

    @Override
    public PRObject createObject(ObjId objId, Object value) {
        AppObject obj = new AppObject(objId, (int) value);
        return obj;
    }


}

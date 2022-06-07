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
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastarv2.Client;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

public class AppClient {
    private static final Logger logger = Logger.getLogger(AppClient.class);
    Client clientProxy;
    int outstanding;
    long startTime;
    ThroughputPassiveMonitor tpMonitor;
    LatencyPassiveMonitor latencyMonitor;
    private Semaphore sendPermits;

    public AppClient(int clientId, String systemConfigFile, String partitioningFile, int outstanding) {
        this.startTime = System.currentTimeMillis();
        clientProxy = new Client(clientId, systemConfigFile, partitioningFile);
        this.outstanding = outstanding;
//        tpMonitor = new ThroughputPassiveMonitor(clientId, "client_tp");
//        latencyMonitor = new LatencyPassiveMonitor(clientId, "client_latency");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length == 5) {
            int clientId = Integer.parseInt(args[0]);
            String systemConfigFile = args[1];
            String partitionsConfigFile = args[2];
            int outstanding = Integer.parseInt(args[3]);
            boolean isInteractive = Boolean.parseBoolean(args[4]);
//            String gathererHost = args[5];
//            int gathererPort = Integer.parseInt(args[6]);
//            String gathererDir = args[7];
//            int gathererDuration = Integer.parseInt(args[8]);
//            int gathererWarmup = Integer.parseInt(args[9]);
//            DataGatherer.configure(gathererDuration, gathererDir, gathererHost, gathererPort, gathererWarmup);
            AppClient appcli = new AppClient(clientId, systemConfigFile, partitionsConfigFile, outstanding);
            System.out.println("Interactive?:" + isInteractive);
            appcli.setPermits(outstanding);
            if (isInteractive) {
//                appcli.setup();
                appcli.runInteractive();
            } else {
                appcli.runBatch();
            }
        } else {
            logger.debug("USAGE: AppClient | clientId | systemConfigFile | partitionConfigFile | outstanding | interactive");
            System.exit(1);
        }

    }

    void setup() {
        for (int i = 0; i < 10; i++) {
            CompletableFuture<Message> cmdEx = clientProxy.create(new AppObject(new ObjId(i), i));
            try {
                cmdEx.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        for (int i = 30; i < 40; i++) {
            CompletableFuture<Message> cmdEx = clientProxy.create(new AppObject(new ObjId(i), i));
            try {
                cmdEx.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        try {
            ObjId oids[] = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(2);
            Command cmd = new Command(AppCommand.ADD, oids);
            CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(2);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(3);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(4);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(5);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(2);
            oids[1] = new ObjId(3);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(2);
            oids[1] = new ObjId(5);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(6);
            oids[1] = new ObjId(7);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(6);
            oids[1] = new ObjId(8);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(1);
            oids[1] = new ObjId(6);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(6);
            oids[1] = new ObjId(9);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(7);
            oids[1] = new ObjId(8);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(7);
            oids[1] = new ObjId(9);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(8);
            oids[1] = new ObjId(9);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(32);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(32);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(33);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(34);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(35);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(32);
            oids[1] = new ObjId(33);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(32);
            oids[1] = new ObjId(35);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(36);
            oids[1] = new ObjId(37);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(36);
            oids[1] = new ObjId(38);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(31);
            oids[1] = new ObjId(36);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(36);
            oids[1] = new ObjId(39);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(37);
            oids[1] = new ObjId(38);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(37);
            oids[1] = new ObjId(39);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();

            oids = new ObjId[2];
            oids[0] = new ObjId(38);
            oids[1] = new ObjId(39);
            cmd = new Command(AppCommand.ADD, oids);
            cmdEx = clientProxy.executeCommand(cmd);
            cmdEx.get();


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    void setPermits(int num) {
        sendPermits = new Semaphore(num);
    }

    void getPermit() {
        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    void addPermit() {
        if (sendPermits != null)
            sendPermits.release();
    }

    public void runBatch() {
        for (int i = 0; i < 100; i++) {
            getPermit();
            ObjId oid = new ObjId(i);
            int value = i;
            long startTime = System.currentTimeMillis();
            CompletableFuture<Message> cmdEx = clientProxy.create(new AppObject(oid, value));
            cmdEx.thenAccept(reply -> {
                addPermit();
                long endTime = System.currentTimeMillis();
                System.out.println((endTime - startTime) + "-" + value + " - " + reply);
            });
        }
    }

    public void runInteractive() throws ExecutionException, InterruptedException {
        Scanner scan = new Scanner(System.in);
        String input;

        System.out.println("input format: a(dd)/s(ub)/m(ul)/d(iv)/c(reate)/de(lete)/r(ead)/rb (read batch) oid1 value (or end to finish)");
        input = scan.nextLine();
        while (!input.equalsIgnoreCase("end")) {
            String[] params = input.split(" ");
            String opStr = params[0];
            ArrayList<Object> args = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            if (opStr.equalsIgnoreCase("c")) {
                ObjId oid = new ObjId(Integer.parseInt(params[1]));
                int value = Integer.parseInt(params[2]);
                CompletableFuture<Message> cmdEx = clientProxy.create(new AppObject(oid, value));
                Message rep = cmdEx.get();
                Set<AppObject> obj = (Set) rep.getItem(0);
                System.out.println(obj);
            } else if (opStr.equalsIgnoreCase("cm")) {
                // create object with dependencies
                ObjId oid = new ObjId(Integer.parseInt(params[1]));
                int value = Integer.parseInt(params[2]);
                AppObject obj = new AppObject(oid, value);
                Set<ObjId> dependencies = new HashSet<>();
                dependencies.add(new ObjId(1));
                dependencies.add(new ObjId(2));
                dependencies.add(new ObjId(3));
                CompletableFuture<Message> cmdEx = clientProxy.create(obj, dependencies);
                Message rep = cmdEx.get();
                Set<AppObject> ret = (Set<AppObject>) rep.getItem(0);
                System.out.println(ret);
            } else if (opStr.equalsIgnoreCase("ar")) {
                ObjId oids[] = new ObjId[params.length - 1];
                for (int i = 1; i < params.length; i++) {
                    oids[i - 1] = new ObjId(Integer.parseInt(params[i]));
                }
                AppCommand o = AppCommand.ADD;
                Command cmd = new Command(o, oids);
                AppObject obj1 = new AppObject(null, 999);
                AppObject obj2 = new AppObject(null, 998);
                Set<PRObject> reservedObjs = new HashSet<>();
                reservedObjs.add(obj1);
                reservedObjs.add(obj2);
                cmd.setReservedObjects(reservedObjs);
                CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
                cmdEx.thenAccept(message -> {
                    long endTime = System.currentTimeMillis();
                    System.out.println((endTime - startTime) + " - " + message);
                });
            } else if (opStr.equalsIgnoreCase("r")) {
                ObjId oid = new ObjId(Integer.parseInt(params[1]));
                CompletableFuture<Message> cmdEx = clientProxy.read(oid);
                System.out.println(cmdEx.get());
            } else if (opStr.equalsIgnoreCase("de")) {
                ObjId oid = new ObjId(Integer.parseInt(params[1]));
                CompletableFuture<Message> cmdEx = clientProxy.delete(oid);
                System.out.println(cmdEx.get());
            } else if (opStr.equalsIgnoreCase("rb")) {
                Set<ObjId> objIds = new HashSet<>();
                for (int i = 1; i < params.length; i++) {
                    objIds.add(new ObjId(Integer.parseInt(params[i])));
                }
                CompletableFuture<Message> cmdEx = clientProxy.read(objIds);
                System.out.println(cmdEx.get());
            } else {
                ObjId oids[] = new ObjId[params.length - 1];
                for (int i = 1; i < params.length; i++) {
                    oids[i - 1] = new ObjId(Integer.parseInt(params[i]));
                }
                AppCommand o = null;
                if (opStr.equalsIgnoreCase("a")) {
                    o = AppCommand.ADD;
                } else if (opStr.equalsIgnoreCase("m")) {
                    o = AppCommand.MUL;
                } else if (opStr.equalsIgnoreCase("s")) {
                    o = AppCommand.SUB;
                } else if (opStr.equalsIgnoreCase("d")) {
                    o = AppCommand.DIV;
                }
                Command cmd = new Command(o, oids);
                CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
//                CompletableFuture<Message> cmdEx = clientProxy.executeSSMRCommand(cmd);
                cmdEx.thenAccept(message -> {
                    long endTime = System.currentTimeMillis();
                    System.out.println((endTime - startTime) + " - " + message);
                });
//                Message rep = cmdEx.get();
//                long endTime = System.currentTimeMillis();
//                System.out.println((endTime - startTime) + " - " + rep);
            }

            input = scan.nextLine();
        }
        scan.close();
    }

}

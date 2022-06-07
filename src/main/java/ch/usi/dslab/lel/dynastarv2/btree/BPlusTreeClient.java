package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastarv2.*;
import ch.usi.dslab.lel.dynastarv2.probject.*;
import ch.usi.dslab.lel.dynastarv2.command.*;
//import org.apache.log4j.Logger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by meslahik on 11.09.17.
 */
public class BPlusTreeClient {
    private static final Logger logger = LoggerFactory.getLogger(BPlusTreeClient.class);

    int clientId;
    final int ROOT_ID = 1;
    Client clientProxy;
    int outstanding;
//    private Semaphore sendPermits;
    Map<Integer, Tree> trees = new HashMap<>();

    public BPlusTreeClient(int clientId, String systemConfigFile, String partitioningFile, int outstanding, int isInteractive) {
        this.clientId = clientId;
        clientProxy = new Client(clientId, systemConfigFile, partitioningFile);
        this.outstanding = outstanding;
        for(int i=0; i < outstanding; i++) {
            int id = clientId + i;
            Tree tree = new Tree(id);
            if (isInteractive != 0) {
                tree.tpMonitor = new ThroughputPassiveMonitor(id, "client_tp");
                tree.latencyMonitor = new LatencyPassiveMonitor(id, "client_latency");
//                tree.timeline = new TimelinePassiveMonitor(id, "client_timeline");
                tree.cacheMissMonitor = new ThroughputPassiveMonitor(id, "client_cache_miss");
                tree.appRequestMonitor = new ThroughputPassiveMonitor(id, "client_app_request");
            }
            trees.put(i, tree);
        }
        System.setErr(System.out);
    }

    public void runBatchInsert(int numOfInserts) {
        for (Tree tree : trees.values()) {
            Thread thread = new Thread(() -> tree.runBatchInsert(numOfInserts));
            thread.setName("CLIENT" + tree.treeId);
            thread.start();
            System.out.println("start client " + tree.treeId + " to insert the keys ...");
        }
    }

    public void runBatchSearch(int numOfSearch) {
        for (Tree tree : trees.values()) {
            Thread thread = new Thread(() -> tree.runBatchSearch(numOfSearch));
            thread.start();
            System.out.println("start client " + tree.treeId + " to search the keys ...");
        }
    }

    public void runBatchMixed(int numOfOperation, int updatePercentage, int insertPercentage, String fileName) {
        for (Tree tree : trees.values()) {
            Thread thread = new Thread(() -> tree.runBatchMixed(numOfOperation, updatePercentage, insertPercentage, fileName));
            thread.start();
            System.out.println("start client " + tree.treeId + " to do the mix operation the keys ...");
        }
    }

    public void runBatchInsertFromFile(String fileName) {
        for (Tree tree : trees.values()) {
            Thread thread = new Thread(() -> tree.runBatchInsertFromFile(fileName));
            thread.start();
            System.out.println("start client " + tree.treeId + " to insert the keys from file: " + fileName);
        }
    }

    public void runBatchUpdate(int numOfUpdates, String fileName) {
        for (Tree tree : trees.values()) {
            Thread thread = new Thread(() -> tree.runBatchUpdate(numOfUpdates, fileName));
            thread.start();
            System.out.println("start client " + tree.treeId + " to update the keys from file: " + fileName);
        }
    }

    void setup() {
        try {
            Thread.sleep(5000);
            logger.debug("creating ROOT object ...");
//        Command command = new Command(GenericCommand.CREATE, ROOT.objId, "ROOT");
            CompletableFuture<Message> cmdEx = clientProxy.create(new BPlusTreeNode(new ObjId(ROOT_ID), "ROOT"));
            try {
                Message msg = cmdEx.get();
                logger.debug(msg.getNext().toString());
                logger.debug("ROOT object has been created");
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.debug("creating root node ...");
//        command = new Command(GenericCommand.CREATE, new ObjId(2), ROOT.objId); // use the value argument for isleaf
            Set<ObjId> dependencies = new HashSet<>();
            dependencies.add(new ObjId(1));
            cmdEx = clientProxy.create(new BPlusTreeNode(new ObjId(2), new ObjId(ROOT_ID)), dependencies);
            try {
                Message msg = cmdEx.get();
                if (msg.hasNext()) {
                    HashSet<BPlusTreeNode> node = (HashSet) msg.getNext();
                    logger.debug(node.toString());
                    if (node != null) {
                        //ROOT.setRootId(node.);
                        logger.debug("root node has been created");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void runInteractive() {
        try {
            Scanner scan = new Scanner(System.in);

            Usage();
            String input = scan.nextLine();
            String[] inputs = input.split(" ");
            while (!inputs[0].equalsIgnoreCase("end")) {
                if (inputs.length != 1 && inputs.length != 2 && inputs.length != 3) {
                    Usage();
                    input = scan.nextLine();
                    inputs = input.split(" ");
                    continue;
                }
                if (inputs[0].equalsIgnoreCase("s")) {
                    int key = Integer.parseInt(inputs[1]);
                    Value val = trees.get(0).Search(key);
                    if (val.isAvailable)
                        System.out.println("Key = " + key + ", Value = " + val.value);
                    else
                        System.out.println("The key is not available");
                } else if (inputs[0].equalsIgnoreCase("i")) {
                    int key = Integer.parseInt(inputs[1]);
                    int value = Integer.parseInt(inputs[2]);
                    trees.get(0).Insert(key, value);
                } else if (inputs[0].equalsIgnoreCase("r")) {
                    int key = Integer.parseInt(inputs[1]);
                    BPlusTreeNode node = trees.get(0).readNodeFromServers(new ObjId(key));
                    if (node != null)
                        System.out.println("received the node:\n" + node.toStringContent());
                    else
                        System.out.println("received node null");
                } else if (inputs[0].equalsIgnoreCase("rr")) {
                    BPlusTreeRoot r = trees.get(0).readRootFromServers();
                    if (r != null)
                        System.out.println("received the node:\n" + r);
                    else
                        System.out.println("received node null");
                } else
                    Usage();

                input = scan.nextLine();
                inputs = input.split(" ");
            }
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Tree {
        private final Logger logger = LoggerFactory.getLogger(Tree.class);
        ThroughputPassiveMonitor tpMonitor;
        LatencyPassiveMonitor latencyMonitor;
//        TimelinePassiveMonitor timeline;
        ThroughputPassiveMonitor cacheMissMonitor;
        ThroughputPassiveMonitor appRequestMonitor;

        final int SEARCH_LIMIT = 20;
        final int SPLITABORT_LIMIT = 20;
        int numOfSearch = 1;
        int numOfSPLITABORT = 1;

        int treeId;
        int failedRead = 0;
        int dropped = 0;
        int searchLimitReached = 0;
        int cacheMiss = 0;

        BPlusTreeRoot ROOT = new BPlusTreeRoot(new ObjId(1));
        HashMap<ObjId, BPlusTreeNode> nodes = new HashMap<>();
//        HashMap<ObjId, BPlusTreeNode> leaves = new HashMap<>(); // it is only used for finding the parent of the node for invalidating
        Map<ObjId, ObjId> childParent = new HashMap<>();

        public Tree(int treeId) {
            this.treeId = treeId;
        }

        void cleanUpCache() {
            nodes.clear();
//            leaves.clear();
            childParent.clear();
            ROOT.setRootId(null);
        }

        void cleanUpCacheNoRoot() {
            nodes.clear();
            childParent.clear();
        }

        void increaseCacheMiss() {
            cacheMiss++;
            cacheMissMonitor.incrementCount();
        }

        void removeSubTree(BPlusTreeNode node) {
            if (node == null)
                return;

            logger.debug("invalidating subtree with root node {}", node.getId());
            if (node.height > 2) { // remove children and last child
                node.children.forEach(x -> removeSubTree(nodes.get(x)));
                removeSubTree(nodes.get(node.lastChild));
            } else if (node.height == 2) {
                node.children.forEach(x -> childParent.remove(x));
                childParent.remove(node.lastChild);
            } else { // only for case the method is called for a leaf node; never happens??
                childParent.remove(node.getId());
//                leaves.remove(node.getId());
            }
            childParent.remove(node.getId());
            nodes.remove(node.getId());

//            if (node.isLeaf) {
//                leaves.remove(node.getId());
//                logger.debug("leaf node {} invalidated from the cache", node);
//            } else {
//                nodes.remove(node.getId());
//                logger.debug("node {} invalidated from the node", node);
//            }
//            logger.debug("subtree with root node {} is invalidated successfully", node);
        }

        /**
         * the idea is to invalidate the parent node which means all the subtree with the root of the parent node is deleted from the cache
         * @param objid the node that its parent need to be invalidated
         */
        void invalidateParent(ObjId objid) {
//            BPlusTreeNode node = leaves.get(objid);
//            if (node == null)
//                node = nodes.get(objid);
//            ObjId poid = node.parent;
            ObjId poid = childParent.get(objid);
            logger.debug("invalidating parent node of the node {} with parent {} from the cache; root node {}", objid, poid, ROOT.getRootId());
//            if (poid == null) // condition 1: parent is null; delete the subtree with root of the node.
//                removeSubTree(node);
            if (poid == null) // condition 1: parent is null; root node
                cleanUpCache();
            else // condition 2: parent is not null;
                removeSubTree(nodes.get(poid));
        }

        void invalidateNode(ObjId objId) {
            if (objId == null)
                return;

            logger.debug("invaliding node {} from the cache", objId);
//            BPlusTreeNode node = leaves.get(objId);
            BPlusTreeNode node = nodes.get(objId);
            if (node == null) {
//                leaves.remove(objId);
                if (childParent.containsKey(objId)) {
                    childParent.remove(objId);
                    logger.debug("leaf node {} invalidated from the cache");
                }
            } else {
//                node = nodes.get(objId);
                removeSubTree(node);
                logger.debug("subtree with root node {} is invalidated successfully", node.getId());
            }
        }

        BPlusTreeRoot readRootFromServers() throws InterruptedException, ExecutionException {
            logger.debug("Send request to receive root object {}");
            CompletableFuture<Message> cmdEx = clientProxy.read(ROOT.getId());

            Message msg = cmdEx.get();
            if (msg == null) { // in case of an invalid READ (theoretically should not happen)
                failedRead++;
                logger.error("received null while reading root object from servers");
                return null;
            }
            return (BPlusTreeRoot) msg.getItem(0);
        }

        BPlusTreeNode readNodeFromServers(ObjId oid) throws InterruptedException, ExecutionException {
            logger.debug("Send request to receive node {}", oid);
            CompletableFuture<Message> cmdEx = clientProxy.read(oid);

            Message msg = cmdEx.get();
            if (msg == null) { // in case of an invalid READ (theoretically should not happen)
                failedRead++;
                logger.error("received null while reading node {} from servers", oid);
                return null;
            }
            else if (msg.getItem(0) instanceof String) {
                System.out.println("received (read cmd): " + msg.getItem(0));
                return null;
            }
            return (BPlusTreeNode) msg.getItem(0);
        }

        BPlusTreeNode processNodeReadFromServers(BPlusTreeNode node, ObjId parentOId, int key) {
            if (node != null && node.isInFenceKeys(key)) {
                // we set the parent ObjId for the local nodes. It makes possible to invalidate the immediate parent in case of invalid read
                // Also we have a structure of nodes. each node knows its parent. In case of split, we know the parent to pass for split operation
//                node.parent = parentOId;
                logger.debug("node {} with lowKey={} and highKey={} with parent node {} for searching the correct leaf of key={} received", node.getId(), node.lowKey, node.highKey, parentOId, key);
                if (!node.isLeaf)
                    nodes.put(node.getId(), node);
//                else
//                    leaves.put(node.getId(), node);
                childParent.put(node.getId(), parentOId);
                return node;
            }
            invalidateNode(parentOId);
            return null;
        }

        /**
         * The key argument is because we don't have the node locally, we need to validate the node received from the server
         * We also send the parent node for the same reason, to invalidate it in case of reading a wrong node
         * if it returns, we are sure that the node is available (locally or received from the server)
         * @param oid object to retrieve
         * @param parentOId parent node of the requested node.
         * @param key the key to validate the requested node.
         * @return node with id = oid
         */
        BPlusTreeNode getNode(ObjId oid, ObjId parentOId, int key) throws InterruptedException, ExecutionException {
            BPlusTreeNode node = nodes.get(oid);
            if (node != null) {
                logger.debug("Cached Node content: {}", node.toStringContent());
                return node;
            } else {
                increaseCacheMiss();
                BPlusTreeNode nodeRead = readNodeFromServers(oid);
                return processNodeReadFromServers(nodeRead, parentOId, key);
            }
        }

//        void getRoot() {
//            logger.debug("Send request to receive root object...");
//            CompletableFuture<Message> cmdEx = clientProxy.read(ROOT.getId());
//
//            try {
//                Message msg = cmdEx.get();
//                ROOT = (BPlusTreeRoot) msg.getItem(0);
//                logger.debug("Received Answer! the root node is {}", ROOT.getRootId());
//            }
//            catch (Exception ex) {
//                dropped++;
//                ex.printStackTrace();
//            }
//        }

        /**
         * This method is a utility for inner functionality of the object. InsertUtil use this to traverse the tree and know the
         * details of node that is searched and the child to go in, in case the node is not leaf.
         */
        public NodeSearchValue SearchUtil(int key, boolean returnLeafParentSearchResult) throws InterruptedException, ExecutionException {
            logger.debug("search for the key= {}", key);
            boolean isCompleted = true;
            NodeSearchValue val;
            BPlusTreeNode node;

            int height;
            if (returnLeafParentSearchResult)
                height = 2;
            else
                height = 1;

//            if (numOfSearch % 20 == 0) {
//                logger.error("cache cleaup");
//                cleanUpCache();
//            }

            while (true) {
                if (ROOT.getRootId() == null) {
                    BPlusTreeRoot r = readRootFromServers();
                    if (r != null)
                        ROOT.setRootId(r.getRootId());
                    else {
                        cleanUpCache();
                        continue;
                    }
                }
                node = getNode(ROOT.getRootId(), null, key);
                if (node == null || !node.isRoot) {
                    logger.debug("root ID is not valid. read ROOT object from oracle");
                    cleanUpCache();
                } else
                    break;
            }

            if (returnLeafParentSearchResult && node.height == 1) // special case when there is only root and operation is insertion
                return new NodeSearchValue(node.getId(), 1, node.getId());

            if (numOfSearch == SEARCH_LIMIT)
                logger.error("SEARCH LIMIT: root is {} with nodeHeight={}, low key={}, high key={}", node.getId(), node.height, node.lowKey, node.highKey);
            logger.debug("current root node in the cache is {}. start looking up the key", node);
            for (val = node.Search(key); val.nodeHeight > height; val = node.Search(key)) {
                logger.debug("Search result: {}", val);
                node = getNode(val.childNodeId, node.getId(), key); // if returns null, whether structure of the tree has changed or if happens repeatedly, there is sth wrong with the structure of the node
                if (node == null) {
                    if (numOfSearch == SEARCH_LIMIT)
                        logger.error("SEARCH LIMIT: node which was wrong: {}", val.childNodeId);
                    isCompleted = false;
                    break;
                }
                if (numOfSearch == SEARCH_LIMIT)
                    logger.error("SEARCH LIMIT: next node is: {} with nodeHeight={}, low key={}, high key={}", node.getId(), node.height, node.lowKey, node.highKey);
            }

            if (isCompleted) {
                numOfSearch = 1;
                logger.debug("search is completed. the result is {}", val);
                return val;
            } else { // if it is not completed, it means that a node was invalidated, so we need to do the search again
                if (numOfSearch < SEARCH_LIMIT) {
                    numOfSearch++;
                    logger.debug("search again for the key {}", key);
                    return SearchUtil(key, returnLeafParentSearchResult);
                } else {
                    numOfSearch = 1;
                    searchLimitReached++;
                    logger.error("unable to find key= {}. Search limit exceeded", key);
                    return null;
                }
            }
        }

        /**
         *
         * @param key the key to search
         * @return contains the value if it is available or sets the isAvailable of Value object to false
         */
        Value Search(int key) throws InterruptedException, ExecutionException {
            NodeSearchValue searchResult = SearchUtil(key, true);

            if (searchResult == null) {
                logger.debug("the search for finding the correct node to search was unsuccessful");
                return null;
            }
//            if (!childParent.containsKey(searchResult.childNodeId) && searchResult.nodeId.value != searchResult.childNodeId.value)
            if (searchResult.nodeId.value != searchResult.childNodeId.value)
                childParent.put(searchResult.childNodeId, searchResult.nodeId);

            logger.debug("send request to search for the key {}", key);
            Command cmd = new Command(BPlusTreeCommand.SEARCH, searchResult.childNodeId, key);
            Message msg = callExecuteCommand(cmd);

            if (msg != null) {
                BPlusTreeCommand cmdResponse = (BPlusTreeCommand) msg.getItem(0);

                if (cmdResponse == BPlusTreeCommand.SEARCHFOUND) {
                    int value = (int) msg.getItem(2);
                    logger.debug("The key={} with value={} is found in the tree", key, value);
                    tpMonitor.incrementCount();

                    Value val = new Value();
                    val.isAvailable = true;
                    val.value = value;
                    return val;
                } else if (cmdResponse == BPlusTreeCommand.SEARCHNOTFOUND) {
                    logger.debug("The key={} is not found in the tree", key);
                    tpMonitor.incrementCount();

                    Value val = new Value();
                    val.isAvailable = false;
                    return val;
                } else if (cmdResponse == BPlusTreeCommand.SEARCHRETRY) {
                    logger.debug("The search was not valid. try again");
                    invalidateParent(searchResult.childNodeId);
                    return Search(key);
                } else {
                    logger.error("should not happen! partition should return one of the cases above.");
                    return null;
                }
            }
            else
                return null;
        }

        Message callExecuteCommand(Command cmd) {
            CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
            try {
                appRequestMonitor.incrementCount();
                Message msg = cmdEx.get();
                if (msg.getItem(0) instanceof String && ((String) msg.getItem(0)).equalsIgnoreCase("DROPPED")) {
                    System.out.println("received (APP cmd): " + msg);
                    dropped++;
                    logger.error("dropping message received");
                    return null;
                } else
                    return msg;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        /**
         *
         * @param msg
         */
        boolean insertUtil(Message msg) throws InterruptedException, ExecutionException {
            logger.debug("processing message {}", msg);
            BPlusTreeCommand cmd = (BPlusTreeCommand) msg.getItem(0);
            ObjId oid = (ObjId) msg.getItem(1);
            int key = (int) msg.getItem(2);
            int value = (int) msg.getItem(3);

            if (cmd == BPlusTreeCommand.UPDATED) {
                logger.debug("The key= {}, value= {} is updated successfully", key, value);
                tpMonitor.incrementCount();
                return true;
            } else if (cmd == BPlusTreeCommand.INSERTED) {
                numOfSPLITABORT = 0;
                logger.debug("The key = {}, value = {} is inserted successfully", key, value);
                tpMonitor.incrementCount();
                return true;
            } else if (cmd == BPlusTreeCommand.INSERTRETRY) { // need to invalidate the parent node
                logger.debug("INSERTRETRY key={}, value={} in node {}", key, value, oid);
                invalidateParent(oid);
                return Insert(key, value);
            } else if (cmd == BPlusTreeCommand.RETRYSPLIT) {
                List<ObjId> parentList = (List<ObjId>) msg.getItem(4);
                List<Integer> newNodeList = (List<Integer>) msg.getItem(5);

                logger.debug("RETRYSPLIT key={}, value={}", key, value);

                // create an array list for parent list
                if (parentList == null)
                    parentList = new ArrayList<>();

                // create an array list for list of new node ids which will be provided by oracle
                if (newNodeList == null)
                    newNodeList = new ArrayList<>();

                // the node that we need to add its parent to parent list
//                BPlusTreeNode lastNode;
                ObjId lastNode;
                if (parentList.size() == 0)
                    lastNode = oid;
                else
                    lastNode = parentList.get(parentList.size() - 1);

                Set<PRObject> reservedObjs = new HashSet<>();
                for (int i = 0; i < newNodeList.size(); i++)
                    reservedObjs.add(new BPlusTreeNode(new ObjId(newNodeList.get(i)), null));
                newNodeList.clear();

                boolean rootSplit;
                logger.debug("last node: {}, root node id: {}", lastNode, ROOT.getRootId());
                if (lastNode.value == ROOT.getRootId().value) {
                    rootSplit = true;
                    reservedObjs.add(new BPlusTreeNode(null, null));
                    reservedObjs.add(new BPlusTreeNode(null, null));
                    logger.debug("request for 2 reserved objects added to the command");
                    logger.debug("parent list: {}", parentList);
                } else {
                    rootSplit = false;
                    reservedObjs.add(new BPlusTreeNode(null, null));
                    logger.debug("request for 1 reserved objects added to the command");
                    parentList.add(childParent.get(lastNode));
                    logger.debug("parent list: {}", parentList);
                }

                Command newcmd;
                if (!rootSplit)
                    newcmd = new Command(BPlusTreeCommand.INSERTSPLIT, oid, key, value, parentList, newNodeList, rootSplit);
                else
                    newcmd = new Command(BPlusTreeCommand.INSERTSPLIT, oid, key, value, parentList, newNodeList, rootSplit, ROOT.getId());

                newcmd.setReservedObjects(reservedObjs);

                Message newmsg = callExecuteCommand(newcmd);
                if (newmsg != null)
                    return insertUtil(newmsg);
                else
                    return false;
            } else if (cmd == BPlusTreeCommand.SPLITRETRY) {
                numOfSPLITABORT++;
                ObjId invalidNodeOId = (ObjId) msg.getItem(4);
//                BPlusTreeNode invalidNode = leaves.get(invalidNodeOId);
//                if (invalidNode == null)
//                    invalidNode = nodes.get(invalidNodeOId);
                if (numOfSPLITABORT > SPLITABORT_LIMIT) {
                    logger.error("unable to INSERTSPLIT for inserting key= {}, value= {} in the node: {}", key, value, oid);
                    logger.error("invalid node: {}", invalidNodeOId);
                    numOfSPLITABORT = 0;
                    return false;
                }

                logger.debug("SPLITRETRY " + key + ":" + value);
                invalidateParent(invalidNodeOId);
                return Insert(key, value);
            }
            return false;
        }

        public boolean Insert(int key, int value) throws InterruptedException, ExecutionException {
            logger.debug("inserting the key={}, value={}", key, value);
            NodeSearchValue searchResult = SearchUtil(key, true);
            if (searchResult == null) {
                logger.debug("the search for finding the correct node to insert was unsuccessful");
                return false;
            }
//            if (!childParent.containsKey(searchResult.childNodeId) && searchResult.nodeId.value != searchResult.childNodeId.value)
            if (searchResult.nodeId.value != searchResult.childNodeId.value)
                childParent.put(searchResult.childNodeId, searchResult.nodeId);

            Command cmd = new Command(BPlusTreeCommand.INSERT, searchResult.childNodeId, key, value);
            Message msg = callExecuteCommand(cmd);

            if (msg != null)
                return insertUtil(msg);
            else
                return false;
        }

//    void setPermits(int num) {
//        sendPermits = new Semaphore(num);
//    }
//
//    void getPermit() {
//        try {
//            sendPermits.acquire();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
//
//    void addPermit() {
//        if (sendPermits != null)
//            sendPermits.release();
//    }

        public void runBatchInsert(int numOfInserts) {
            try {
                Thread.sleep(5000);
                Random random = new Random();
                for (int i = 0; i < numOfInserts; i++) {
//                    cleanUpCacheNoRoot();
                    int rand = random.nextInt() % 200000000;
                    if (rand < 0)
                        rand = rand * (-1);
                    long timeSent = System.currentTimeMillis();
                    boolean res = Insert(rand, rand * 10);
                    long timeReceived = System.currentTimeMillis();
                    latencyMonitor.logLatency(timeSent, timeReceived);
                    if (!res)
                        logger.error("unable to insert key= {}, value= {} in the {}th insert", rand, rand * 10, i);
                }
                System.out.println("[" + treeId + "] All insertions done! " +
                        "dropping messages: " + dropped +
                        ", failed read:" + failedRead +
                        ", search limit: " + searchLimitReached +
                        ", cache Miss: " + cacheMiss);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void runBatchSearch(int numOfSearch) {
            try {
                Thread.sleep(5000);
                Random random = new Random();
                for (int i = 0; i < numOfSearch; i++) {
//                    cleanUpCacheNoRoot();
                    int rand = random.nextInt() % 200000000;
                    if (rand < 0)
                        rand = rand * (-1);
                    long timeSent = System.currentTimeMillis();
                    Value val = Search(rand);
                    long timeReceived = System.currentTimeMillis();
                    latencyMonitor.logLatency(timeSent, timeReceived);
//                    if (!val.isAvailable)
//                        logger.error("unable to search key= {} in the {}th search", rand, i);
                }
                System.out.println(treeId + ": All searches done! " + "cache Miss: " + cacheMiss);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void runBatchMixed(int numOfOperations, int insertPercentage, int updatePercentage, String fileName) {
            try {
                Thread.sleep(5000);
                Random random = new Random();

                ArrayList<Integer> randKeys = null;
                int randKeysSize = 0;
                if (updatePercentage != 0 && !fileName.equalsIgnoreCase("-")) {
                    randKeys = RandomKeysUtil.readFromFile(fileName);
                    randKeysSize = randKeys.size();
                }

                int numCmdsIndex = 0;
                int numInsertsIndex = 0;
                int numUpdatesIndex = 0;

                for (int i = 0; i < numOfOperations; i++) {

                    if (numCmdsIndex == 0) {
                        numCmdsIndex = 100;
                        numInsertsIndex = insertPercentage;
                        numUpdatesIndex = updatePercentage;
                    }

                    if (numInsertsIndex != 0) {
                        int rand = random.nextInt() % 200000000;
                        rand = rand >= 0 ? rand : rand*(-1);

                        long timeSent = System.currentTimeMillis();
                        boolean res = Insert(rand, rand * 10);
//                        System.out.println("INSERT");
                        long timeReceived = System.currentTimeMillis();
                        latencyMonitor.logLatency(timeSent, timeReceived);

                        numInsertsIndex--;
                    } else if (numUpdatesIndex != 0) {
                        int rand = random.nextInt();
                        rand = rand >= 0 ? rand : rand*(-1);
                        int keyIndex = rand % randKeysSize;
                        int key = randKeys.get(keyIndex);

                        long timeSent = System.currentTimeMillis();
                        boolean res = Insert(key, key * 10);
                        long timeReceived = System.currentTimeMillis();
                        latencyMonitor.logLatency(timeSent, timeReceived);

                        numUpdatesIndex--;
                    } else {
                        int rand = random.nextInt() % 200000000;
                        rand = rand >= 0 ? rand : rand*(-1);

                        long timeSent = System.currentTimeMillis();
                        Value val = Search(rand);
//                        System.out.println("search");
                        long timeReceived = System.currentTimeMillis();
                        latencyMonitor.logLatency(timeSent, timeReceived);
                    }
                    numCmdsIndex--;
                }
                System.out.println(treeId + ": All operations done! " + "cache Miss: " + cacheMiss);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void runBatchInsertFromFile(String fileName) {
            try {
                Thread.sleep(5000);
                ArrayList<Integer> randKeys = RandomKeysUtil.readFromFile(fileName);
                int numOfInserts = randKeys.size();
                for (int i = 0; i < numOfInserts; i++) {
                    int key = randKeys.get(i);
                    long timeSent = System.currentTimeMillis();
                    boolean res = Insert(key, key * 10);
                    long timeReceived = System.currentTimeMillis();
                    latencyMonitor.logLatency(timeSent, timeReceived);
                    if (!res)
                        logger.error("unable to insert key= {}, value= {} in the {}th insert", key, key * 10, i);
                }
                System.out.println("[" + treeId + "] All insertions done! " +
                        "dropping messages: " + dropped +
                        ", failed read:" + failedRead +
                        ", search limit: " + searchLimitReached +
                        ", cache Miss: " + cacheMiss);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void runBatchUpdate(int numOfUpdates, String fileName) {
            try {
                Random random = new Random();
                ArrayList<Integer> randKeys = RandomKeysUtil.readFromFile(fileName);
                int randKeysSize = randKeys.size();
                for (int i = 0; i < numOfUpdates; i++) {
                    int rand = random.nextInt();
                    rand = rand >= 0 ? rand : rand*(-1);
                    int keyIndex = rand % randKeysSize;
                    int key = randKeys.get(keyIndex);
                    long timeSent = System.currentTimeMillis();
                    boolean res = Insert(key, key * 10);
                    long timeReceived = System.currentTimeMillis();
                    latencyMonitor.logLatency(timeSent, timeReceived);
                    if (!res)
                        logger.error("unable to update key= {}, value= {} in the {}th update", key, key * 10, i);
                }
                System.out.println("[" + treeId + "] All updates done! " +
                        "dropping messages: " + dropped +
                        ", failed read:" + failedRead +
                        ", search limit: " + searchLimitReached +
                        ", cache Miss: " + cacheMiss);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void Usage(){
        System.out.println("Input format: s(search) key - i(insert) key value - end(exit)");
    }

    public static void main(String[] args) {
        if (args.length == 12 || args.length == 13 || args.length == 14 || args.length == 15) {
            int clientId = Integer.parseInt(args[0]);
            String systemConfigFile = args[1];
            String partitionsConfigFile = args[2];
            int outstanding = Integer.parseInt(args[3]);
            //boolean isInteractive = Boolean.parseBoolean(args[4]);
            int isInteractive = Integer.parseInt(args[4]);
            int numOfOperation = Integer.parseInt(args[5]);
            boolean gatherer = Boolean.parseBoolean(args[6]);
            if (gatherer) {
                String gathererHost = args[7];
                int gathererPort = Integer.parseInt(args[8]);
                String gathererDir = args[9];
                int gathererDuration = Integer.parseInt(args[10]);
                int gathererWarmup = Integer.parseInt(args[11]);
                DataGatherer.configure(gathererDuration, gathererDir, gathererHost, gathererPort, gathererWarmup);
            }
            BPlusTreeClient client = new BPlusTreeClient(clientId, systemConfigFile, partitionsConfigFile, outstanding, isInteractive);

//            client.setPermits(outstanding);
            if (isInteractive == 0) {
                client.setup();
                System.out.println("Root Object has been created!");
            }
            else if (isInteractive == 1)
                client.runInteractive();
            else if (isInteractive == 2)
                client.runBatchInsert(numOfOperation);
            else if (isInteractive == 3)
                client.runBatchSearch(numOfOperation);
            else if (isInteractive == 4) {
                int insertPercentage = Integer.parseInt(args[12]);
                int updatePercentage = Integer.parseInt(args[13]);
                String fileName = "-";
                if (args.length > 14)
                     fileName = args[14];
                client.runBatchMixed(numOfOperation, insertPercentage, updatePercentage, fileName);
            }
            else if (isInteractive == 5) {
                String fileName = args[12];
                client.runBatchInsertFromFile(fileName);
            } else if (isInteractive == 6) {
                String fileName = args[12];
                client.runBatchUpdate(numOfOperation, fileName);
            }
        } else {
            System.out.println("USAGE: BPlusTreeClient clientId systemConfigFile partitionConfigFile outstanding interactive numOfOperation " +
                    "gatherer gathererHost gathererPort gathererDir gathererDuration gathererWarmup [fileName]");
            System.exit(1);
        }
    }
}

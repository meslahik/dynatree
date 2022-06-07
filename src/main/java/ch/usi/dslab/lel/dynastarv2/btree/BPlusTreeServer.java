package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.OracleMovePassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.TimelinePassiveMonitor;
import ch.usi.dslab.lel.dynastarv2.*;
import ch.usi.dslab.lel.dynastarv2.command.*;
import ch.usi.dslab.lel.dynastarv2.probject.*;
import ch.usi.dslab.lel.dynastarv2.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by meslahik on 07.09.17.
 */
public class BPlusTreeServer extends PartitionStateMachine {
    static final boolean is64bit = true;
    //private static final Logger logger = Logger.getLogger(BPlusTreeServer.class);
    private static final Logger logger = LoggerFactory.getLogger(BPlusTreeServer.class);
    int minDeg;
    OracleMovePassiveMonitor oracleMovePassiveMonitor;
    ThroughputPassiveMonitor nodeSplitMonitor;

    public BPlusTreeServer(int serverId, String systemConfig, String partitionsConfig, int minDeg) {
        super(serverId, systemConfig, partitionsConfig);
        this.minDeg = minDeg;
        oracleMovePassiveMonitor = new OracleMovePassiveMonitor(serverId, "root_split");
        nodeSplitMonitor = new ThroughputPassiveMonitor(serverId, "node_split");
    }

    public static void main(String[] args) {
        logger.debug("Starting server...");
        if (args.length != 10) {
            System.out.println("USAGE: BPlusTreeServer serverId systemConfigFile partitioningFile minDeg gatherer gathererHost gathererPort gathererDir gathererDuration gathererWarmup");
            System.exit(1);
        }

        int serverId = Integer.parseInt(args[0]);
        String systemConfigFile = args[1];
        String partitionsConfigFile = args[2];
        int minDeg = Integer.parseInt(args[3]);
        boolean gatherer = Boolean.parseBoolean(args[4]);
        if (gatherer) {
            int i = 5;
            String gathererHost = args[i];
            int gathererPort = Integer.parseInt(args[i + 1]);
            String gathererDir = args[i + 2];
            int gathererDuration = Integer.parseInt(args[i + 3]);
            int gathererWarmup = Integer.parseInt(args[i + 4]);
            DataGatherer.configure(gathererDuration, gathererDir, gathererHost, gathererPort, gathererWarmup);
        }

        BPlusTreeServer server = new BPlusTreeServer(serverId, systemConfigFile, partitionsConfigFile, minDeg);
        System.setErr(System.out);

        server.runStateMachine();
    }

//    int linkfollowUp;
//    int lastLinkFollowUp() {
//        return linkfollowUp;
//    }

    BPlusTreeNode getNode(ObjId objId) {
        return (BPlusTreeNode) objectGraph.getPRObject(objId);
    }

    BPlusTreeNode getNode(ObjId objId, int key) {
//        linkfollowUp = 0;
        logger.debug("find node {} for the key {}", objId, key);
        int links = 0;
        BPlusTreeNode node = (BPlusTreeNode) objectGraph.getPRObject(objId);
        while (node != null && node.highKey < key && node.next != null) {
            logger.debug("find node {} for the key {} - next link {}", objId, key, node.next);
            node = (BPlusTreeNode) objectGraph.getPRObject(node.next.getLink());
            links++;
//            if (links > 10) {
//                node = null;
//                break;
//            }
        }
//        if (node != null)
//            logger.error("with {} links ended up in node {} for the key {}", links, node.getId(), key);
//        else
//            logger.error("with {} link could not found the node for key {}", links, key);
        return node;
    }

    @Override
    public Message executeCommand(Command cmd) {
        try {
            cmd.rewind();
            // Command format : | byte op | ObjId o1 | int value |
            MessageType op = (MessageType) cmd.getNext();
            switch ((BPlusTreeCommand) op) {
                case SEARCH: {
                    logger.debug("command SEARCH {} received for key {} in node {}", cmd, cmd.getItem(2), cmd.getItem(1));
                    int key = (int) cmd.getItem(2);
                    BPlusTreeNode node = getNode((ObjId)cmd.getItem(1));
                    if (node == null) {
                        ObjId invalidNode = (ObjId) cmd.getItem(1);
                        BPlusTreeNode nidnode = (BPlusTreeNode) objectGraph.getPRObject(invalidNode);
                        logger.error("searching for key= {} in node {} was wrong. SEARCHRETRY", key, invalidNode);
                        logger.error("nid {} low key {} high key {}", nidnode.getId(), nidnode.lowKey, nidnode.highKey);
                        return new Message(BPlusTreeCommand.SEARCHRETRY, key);
                    }
                    if (!node.isInFenceKeys(key)) {
                        ObjId invalidNode = (ObjId) cmd.getItem(1);
                        logger.debug("something went wrong while searching for the key {} in node {}", key, invalidNode);
                        return new Message(BPlusTreeCommand.SEARCHRETRY, invalidNode, key);
                    }

                    Value val = node.Search(key).result;
                    if (val.isAvailable) {
                        logger.debug("key {} found in node {}", key, node.getId());
                        return new Message(BPlusTreeCommand.SEARCHFOUND, key, val.value);
                    } else {
                        logger.debug("key {} not found in node {}", key, node.getId());
                        return new Message(BPlusTreeCommand.SEARCHNOTFOUND, key);
                    }
                }
                case INSERT: {
                    logger.debug("command INSERT received: {}", cmd);
//                    BPlusTreeNode node = (BPlusTreeNode) objectGraph.getPRObject((ObjId) cmd.getItem(1));
                    int key = (int) cmd.getItem(2);
                    int value = (int) cmd.getItem(3);
                    ObjId nid = (ObjId)cmd.getItem(1);
                    BPlusTreeNode node = getNode(nid);
                    if (node == null) {
                        ObjId invalidNode = nid;
                        logger.error("inserting key= {}, value= {} in node {} was wrong. INSERTRETRY", key, value, invalidNode);
                        BPlusTreeNode nidNode = (BPlusTreeNode) objectGraph.getPRObject(nid);
                        logger.error("nid: {}, low key: {}, high key: {}, next: {}", nidNode.getId(), nidNode.lowKey, nidNode.highKey, nidNode.next);
                        return new Message(BPlusTreeCommand.INSERTRETRY, nid, key, value);
                    }
                    if (!node.isInFenceKeys(key)){
                        ObjId invalidNode = nid;
                        logger.debug("something went wrong while inserting key {}, value {} in node {}", key, value, invalidNode);
                        return new Message(BPlusTreeCommand.INSERTRETRY, nid, key, value);
                    }

                    if (node.Search(key).result.isAvailable) {
                        node.Update(key, value);
                        logger.debug("inserting key= {}, value= {} in node {} was successful. UPDATED", key, value, node.getId());
                        return new Message(BPlusTreeCommand.UPDATED, node.getId(), key, value);
                    } else if (node.getSize() < 2 * minDeg - 1) {
                        node.Insert(key, value);
                        logger.debug("inserting key= {}, value= {} in node {} was successful. INSERTED", key, value, node.getId());
                        return new Message(BPlusTreeCommand.INSERTED, node.getId(), key, value);
                    } else {
                        logger.debug("inserting key= {}, value= {} in node {} needs split. RETRYSPLIT", key, value, node.getId());
                        return new Message(BPlusTreeCommand.RETRYSPLIT, nid, key, value, null, null, node);
                    }
                }
                case INSERTSPLIT: {
                    logger.debug("command INSERTSPLIT received: {}", cmd);
//                    BPlusTreeNode node = (BPlusTreeNode) objectGraph.getPRObject((ObjId) cmd.getItem(1));
                    int key = (int) cmd.getItem(2);
                    int value = (int) cmd.getItem(3);
                    ObjId nid = (ObjId) cmd.getItem(1);
                    BPlusTreeNode node = getNode(nid);
                    if (node == null) {
                        ObjId invalidNode = nid;
                        logger.debug("INSERTSPLIT failed because of invalid node {}. SPLITRETRY", invalidNode);
                        return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                    }
                    if (!node.isInFenceKeys(key)) {
                        ObjId invalidNode = nid;
                        logger.debug("something went wrong while inserting key {}, value {} in node {}", key, value, invalidNode);
                        return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                    }

                    if (node.Search(key).result.isAvailable) {
                        logger.debug("the key is available in the tree. just update the value");
                        node.Update(key, value);
                        return new Message(BPlusTreeCommand.UPDATED, node.getId(), key, value);
                    }
                    // check size for node and all parents. if they have capacity => a simple insert and/or splits only in the ones that does not have capacity
                    if (node.getSize() < 2 * minDeg - 1) {
                        logger.debug("Although the command is INSERTSPLIT, the node {} size={} allows new insertion", node.getId(), node.getSize());
                        node.Insert(key, value);
                        return new Message(BPlusTreeCommand.INSERTED, node.getId(), key, value);
                    }

                    List<ObjId> parentList = (List<ObjId>) cmd.getItem(4);
                    List<Integer> newnodes = (List<Integer>) cmd.getItem(5);
                    boolean rootSplit = (boolean) cmd.getItem(6);
                    BPlusTreeRoot root = null;
                    if (rootSplit)
                        root = (BPlusTreeRoot) objectGraph.getPRObject((ObjId) cmd.getItem(7));

                    Set<PRObject> reservedObjs = cmd.getReservedObjects();
                    newnodes.addAll(reservedObjs.stream().map(x -> x.getId().value).collect(Collectors.toList()));

                    ArrayList<ObjId> parentListTemp = new ArrayList<>();
//                    parentListTemp.addAll(parentList);
                    boolean listTrimmed = false;
                    for (int i = 0; i < parentList.size() - 1; i++) { // last one should have capacity; does not need check
//                        BPlusTreeNode pcheck = (BPlusTreeNode) objectGraph.getPRObject(parentList.get(i));
                        BPlusTreeNode pcheck = getNode(parentList.get(i));
                        if (pcheck == null) {
                            ObjId invalidNode = parentList.get(i);
                            logger.debug("INSERTSPLIT failed because of invalid node {}. SPLITRETRY", invalidNode);
                            return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                        }
                        if (!pcheck.isInFenceKeys(key)) {
                            ObjId invalidNode = pcheck.getId();
                            logger.error("something went wrong while inserting key {}, value {} in node {}", key, value, invalidNode);
                            return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                        }
                        parentListTemp.add(pcheck.getId());
                        if (pcheck.getSize() < 2 * minDeg - 1) {
                            rootSplit = false;
                            listTrimmed = true;
//                            for (int j = parentListTemp.size() - 1; j > i; j--)
//                                parentListTemp.remove(j);
                            logger.debug("the parent list was {}, however parent node {} size={} allows insertion. the new parent list is {}", parentList, pcheck.getId(), pcheck.getSize(), parentListTemp);
                            break;
                        }
                    }
                    if (parentList.size() != 0 && !listTrimmed) {
//                        parentListTemp.add(parentList.get(parentList.size() - 1));
                        ObjId pcheckId = parentList.get(parentList.size() - 1);
                        BPlusTreeNode pcheck = getNode(pcheckId);
                        if (pcheck == null) {
                            ObjId invalidNode = pcheckId;
                            logger.debug("INSERTSPLIT failed because of invalid node {}. SPLITRETRY", invalidNode);
                            return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                        }
                        if (!pcheck.isInFenceKeys(key)) {
                            ObjId invalidNode = pcheck.getId();
                            logger.error("something went wrong while inserting key {}, value {} in node {}", key, value, invalidNode);
                            return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                        }
                        parentListTemp.add(pcheck.getId());
                    }

//                    ObjId invalidNode = null;
//                    boolean flagIsInRange = true; // to see if the key is in the range of the node and all its parents
//                    if (!node.isInFenceKeys(key)) {
//                        flagIsInRange = false;
//                        invalidNode = node.getId();
//                        logger.debug("invalid node: the key {} is not in the range of node {} which is [{},{}]", key, node.getId(), node.lowKey, node.highKey);
//                    }
//                    for (int i = 0; flagIsInRange && i < parentListTemp.size(); i++) {
//                        BPlusTreeNode tmpnode = (BPlusTreeNode) objectGraph.getPRObject(parentListTemp.get(i));
//                        if (!tmpnode.isInFenceKeys(key)) {
//                            flagIsInRange = false;
//                            invalidNode = parentListTemp.get(i);
//                            logger.debug("invalid node: the key {} is not in the range of node {} which is [{},{}]", key, tmpnode.getId(), tmpnode.lowKey, tmpnode.highKey);
//                        }
//                    }

                    BPlusTreeNode lastNode;
                    if (parentListTemp.size() == 0)
                        lastNode = node;
                    else
                        lastNode = (BPlusTreeNode) objectGraph.getPRObject(parentListTemp.get(parentListTemp.size() - 1));

                    // check if the rootsplit that comes from the client is correct
                    // TODO: ROOT object is sent by the client... never used. Is that needed?
                    if (rootSplit && !lastNode.isRoot) {
                        ObjId invalidNode = lastNode.getId();
                        logger.debug("rootsplit sent by the client for the node {}, but the root node is {}", lastNode.getId(), root.getRootId());
                        return new Message(BPlusTreeCommand.SPLITRETRY, nid, key, value, invalidNode);
                    }

                    if (lastNode.getSize() >= 2 * minDeg - 1 && !rootSplit) {
                        logger.debug("INSERTSPLIT failed because last parent node needs split. RETRYSPLIT");
                        logger.debug("node: {}, parent list: {}, parent list temp: {}, lastNode: {}, rootSplit? {}", nid, parentList, parentListTemp, lastNode, rootSplit);
                        return new Message(BPlusTreeCommand.RETRYSPLIT, nid, key, value, parentList, newnodes, node);
                    }

//                    logger.error("parentList: {}, parentListTemp: {}", parentList, parentListTemp);

                    // The actual procedure for splitting the node and insertion
                    BPlusTreeNode nodeToInsert = null;
                    BPlusTreeNode nodeToSplit;
                    //int i = newnodes.size()-1;
                    int i = parentListTemp.size() - 1;

                    if (rootSplit) {
                        logger.debug("start splitting root node {}", root.getRootId());


                        BPlusTreeNode createdNodeParent = createNode(new ObjId(newnodes.get(i + 2)), false);
                        BPlusTreeNode createdNode = createNode(new ObjId(newnodes.get(i + 1)), lastNode.isLeaf);
                        lastNode.Split(minDeg, createdNodeParent, createdNode, true);
                        createdNodeParent.isRoot = true;
                        lastNode.isRoot = false;
                        root.setRootId(createdNodeParent.getId());
                        if (i == -1) { // for the case that there is only the root to split
                            if (lastNode.isInFenceKeys(key))
                                nodeToInsert = lastNode;
                            else if (createdNode.isInFenceKeys(key))
                                nodeToInsert = createdNode;
                            else {
                                logger.error("Should not happen(root). Something went wrong! i=0");
                                logger.error("key= {}. lastNode={}, lowkey={}, highkey={}. createdNode={}, lowkey={}, highkey={}", key, lastNode.getId(), lastNode.lowKey, lastNode.highKey, createdNode.getId(), createdNode.lowKey, createdNode.highKey);
                            }
                        } else if (i >= 0 && lastNode.isInFenceKeys(key)) {
                            // nothing to do
                        } else if (i >= 0 && !lastNode.isInFenceKeys(key) && createdNode.isInFenceKeys(key)) // if the key is not in the fence keys of the last node, we should replace it with newly created node
                            parentListTemp.set(i, createdNode.getId());
                        else {
                            logger.error("Should not happen(root). Something went wrong! i>0");
                            logger.error("key= {}. lastNode={}, lowkey={}, highkey={}. createdNode={}, lowkey={}, highkey={}", key, lastNode.getId(), lastNode.lowKey, lastNode.highKey, createdNode.getId(), createdNode.lowKey, createdNode.highKey);
                        }
                        oracleMovePassiveMonitor.logMessage("RootSplit");
                        nodeSplitMonitor.incrementCount();
                        logger.debug("root splitted successfully!");
                        logger.debug("new root: {}", createdNodeParent.toStringContent());
                        logger.debug("old root: {}", lastNode.toStringContent());
                        logger.debug("new sibling: {}", createdNode.toStringContent());
                    }

                    for (; i >= 0; i--) {
                        if (i == 0) {
                            nodeToSplit = node;
                        } else
                            nodeToSplit = (BPlusTreeNode) objectGraph.getPRObject(parentListTemp.get(i - 1));
                        logger.debug("start splitting node {}", nodeToSplit);

                        BPlusTreeNode createdNode = createNode(new ObjId(newnodes.get(i)), nodeToSplit.isLeaf);
                        BPlusTreeNode splitParent = (BPlusTreeNode) objectGraph.getPRObject(parentListTemp.get(i));
                        nodeToSplit.Split(minDeg, splitParent, createdNode, false);
                        if (i == 0) { // for the last split to determine which node to insert the key
                            if (nodeToSplit.isInFenceKeys(key))
                                nodeToInsert = nodeToSplit;
                            else if (createdNode.isInFenceKeys(key))
                                nodeToInsert = createdNode;
                            else {
                                logger.error("Should not happen. Something went wrong! i=0");
                                logger.error("key= {}. nodeToSplit={}, lowkey={}, highkey={}. createdNode={}, lowkey={}, highkey={}", key, nodeToSplit.getId(), nodeToSplit.lowKey, nodeToSplit.highKey, createdNode.getId(), createdNode.lowKey, createdNode.highKey);
                            }
                        } else if (i > 0 && nodeToSplit.isInFenceKeys(key)) {
                            // nothing to do
                        } else if (i > 0 && !nodeToSplit.isInFenceKeys(key) && createdNode.isInFenceKeys(key)) // there is two option: 1) the parent of the node is still correct 2) the node is the child of the newly created node during the split
                            parentListTemp.set(i - 1, createdNode.getId()); // change the parent node to the newly created one (for the next loop)
                        else {
                            logger.error("Should not happen. Something went wrong! i>0");
                            logger.error("key= {}. nodeToSplit={}, lowkey={}, highkey={}. createdNode={}, lowkey={}, highkey={}", key, nodeToSplit.getId(), nodeToSplit.lowKey, nodeToSplit.highKey, createdNode.getId(), createdNode.lowKey, createdNode.highKey);
                        }

                        nodeSplitMonitor.incrementCount();
                        logger.debug("Node {} splitted successfully! nodes's new values: {}", nodeToSplit.getId(), nodeToSplit.toStringContent());
                        logger.debug("parent node {} values: {}", splitParent, splitParent.toStringContent());
                        logger.debug("New node {} values: {}", createdNode.getId(), createdNode.toStringContent());
                    }
                    nodeToInsert.Insert(key, value);
                    logger.debug("key= {}, value= {} is inserted in {} with lowkey={} and highkey={} successfully", key, value, nodeToInsert, nodeToInsert.lowKey, nodeToInsert.highKey);
                    return new Message(BPlusTreeCommand.INSERTED, node.getId(), key, value);
                }
                default:
                    logger.debug("Command is not defined");
                    return new Message();
            }
        } catch (Exception ex){
            ex.printStackTrace();
//            System.exit(-1);
            throw ex;
        }
    }

    BPlusTreeNode createNode(ObjId oid, boolean isLeaf) {
        try {
            logger.debug("creating node {} isleaf={}", oid, isLeaf);
            BPlusTreeNode newnode = new BPlusTreeNode(oid, null);
            newnode.isLeaf = isLeaf;
//            newnode.setId(oid, this.partitionId); // it sets the new node with id in PRObject's objectindex
            PRObjectNode node = new PRObjectNode(newnode, newnode.getId(), getPartitionId());
            node.setOwnerPartitionId(this.partitionId);
            this.objectGraph.addNode(node);
            logger.info("Node {} created successfully!", newnode.getId());
            return newnode;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    /**
     * I use this function twice, by the setup method in client which means for the first node and special ROOT object creation.
     * So, the created node is root
     *
     * @param objId node ID
     * @param value is the new node leaf or not
     * @return
     */
    @Override
    public PRObject createObject(ObjId objId, Object value) {
//        if (value instanceof String && ((String) value).equalsIgnoreCase("ROOT")) {
//            logger.debug("creating ROOT object");
//            BPlusTreeRoot root = new BPlusTreeRoot((ObjId) objId);
////                root.setId((ObjId) objId, this.partitionId);
//            PRObjectNode node = new PRObjectNode(root, root.getId(), getPartitionId());
//            this.objectGraph.addNode(node);
//            logger.debug("ROOT object {} created successfully", root);
//            return root;
//        } else {
//            logger.debug("creating first node of the tree");
//            ObjId rootId = (ObjId) value;
//            ObjId firstRootId = (ObjId) objId;
//            BPlusTreeRoot root = (BPlusTreeRoot) objectGraph.getPRObject(rootId);
//            BPlusTreeNode node = createNode(firstRootId, true);
//            node.isRoot = true;
//            root.setRootId(firstRootId);
//            logger.debug("First node with id= {} created successfully. ROOT is {}", firstRootId, root);
//            return node;
//        }
        return null;
    }

    @Override
    protected PRObject createObject(PRObject prObject) {
//        super.createObject(prObject);
        try {
            BPlusTreeNode obj = (BPlusTreeNode) prObject;
            if (obj.value instanceof String && ((String) obj.value).equalsIgnoreCase("ROOT")) {
                logger.debug("creating ROOT object");
                BPlusTreeRoot ROOT = new BPlusTreeRoot(obj.getId());
//            PRObjectNode node = new PRObjectNode(obj, obj.getId(), getPartitionId());
//            this.objectGraph.addNode(node);
                logger.debug("ROOT object {} created successfully", ROOT);
                return ROOT;
            } else {
                logger.debug("creating first node of the tree");
//            BPlusTreeNode node = createNode(obj.nodeId, true);
                BPlusTreeNode node = new BPlusTreeNode(obj.getId(), null);
                node.isLeaf = true;
                node.isRoot = true;
                BPlusTreeRoot ROOT = (BPlusTreeRoot) objectGraph.getPRObject((ObjId) obj.value);
                ROOT.setRootId(node.getId());
                logger.debug("First node with id= {} created successfully. ROOT is {}", node.getId(), ROOT);
                return node;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}

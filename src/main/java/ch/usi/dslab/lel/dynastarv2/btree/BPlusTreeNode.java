package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

class BPlusTreeNode extends PRObject {
    private static final Logger logger = LoggerFactory.getLogger(BPlusTreeNode.class);

//    ObjId parent; // only in client side
    //ObjId nodeId;
    boolean isLeaf;
    boolean isRoot;
    ArrayList<Integer> keys = new ArrayList<>();
    ArrayList<Integer> values = new ArrayList<>();
    ArrayList<ObjId> children = new ArrayList<>();
    ObjId lastChild; // TODO: is it possible to only one node would have first children (others' are null)? yes, the leftmost branch of the tree

    // Fence keys
    int lowKey = 0;
    int highKey = 2147483647;

    int height = 1;

    Object value; // It is only used for creation of two objects: special ROOT object and the first Node which is root

    Blink next = new Blink();

    public BPlusTreeNode() {

    }

    public BPlusTreeNode(ObjId oid, Object value) {
        //nodeId = oid;
        setId(oid);
        this.value = value;
        System.setErr(System.out);
    }

    public BPlusTreeNode(int lowKey, int highKey) {
        this.lowKey = lowKey;
        this.highKey = highKey;
        System.setErr(System.out);
    }

    public String toStringContent() {
        return "(nodeid=" + getId() + ", nodeHeight=" + height + ", lowkey=" + lowKey + ", highkey=" + highKey +
                "\nkeys=" + keys.toString() + "\nchildren=" + children + ", last child= " + lastChild + "\nvalues=" + values + ")";
    }

    int getSize() { return keys.size(); }

    boolean isInFenceKeys(int key) {
        if (key >= lowKey && key <= highKey)
            return true;
        return false;
    }

    NodeSearchValue Search(int key) {
        try {
            int i = 0;
            if (isLeaf) {
                logger.debug("look up for the key {} in the leaf node {}", key, getId());
//                for (; i < keys.size() && key > keys.get(i); i++)
//                    continue;
//                if (i < keys.size() && keys.get(i) == key) {
//                    logger.debug("the key exits in the the node {}", getId());
//                    return new NodeSearchValue(getId(), values.get(i));
//                }
                i = Collections.binarySearch(keys, key);
                if (i >= 0 ) {
                    logger.debug("the key exits in the the node {}", getId());
                    return new NodeSearchValue(getId(), values.get(i));
                }
                logger.debug("the key does not exist in the node {}", getId());
                return new NodeSearchValue(getId());
            } else {
                logger.debug("look up for the key {} in the inner node {}", key, getId());
//                for (; i < keys.size() && key >= keys.get(i); i++)
//                    continue;
                i = Collections.binarySearch(keys, key);
                if (i >= 0 && keys.get(i) == key)
                    i++;
                else if (i < 0)
                    i = (i * (-1)) - 1;
                if (i < keys.size()) {
                    ObjId child = children.get(i);
                    logger.debug("the next node for looking up is {}", child);
                    return new NodeSearchValue(getId(), height, child);
                } else {
                    logger.debug("the next node for looking up is {}", lastChild);
                    return new NodeSearchValue(getId(), height, lastChild);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    /**
     * for use with split; this instance is empty and it is going to receive items from its left sibling
     * @param node left sibling to move items from
     * @param start the key to start the move from
     * @param end the key that is end of move
     */
    void move(BPlusTreeNode node, int start, int end) {
        try {
            if (isLeaf) {
                logger.debug("move keys between leaf nodes from {} to {}", node.getId(), getId());
                logger.debug("sender keys: {}", node.keys);
                logger.debug("sender values: {}", node.values);
                for (int j = 0, i = start; i <= end; i++, j++) {
                    keys.add(j, node.keys.get(i));
                    values.add(j, node.values.get(i));
                }
                for (int i = end; i >= start; i--) {
                    node.keys.remove(i);
                    node.values.remove(i);
                }
                logger.debug("sender keys: {}", node.keys);
                logger.debug("sender values: {}", node.values);
                logger.debug("receiver keys: {}", keys);
                logger.debug("receiver values: {}", values);
            } else {
                logger.debug("move keys between inner nodes from {} to {}", node.getId(), getId());
                for (int j = 0, i = start; i <= end; i++, j++) {
                    keys.add(j, node.keys.get(i));
                    children.add(j, node.children.get(i));
                }
                for (int i = end; i >= start; i--) {
                    node.keys.remove(i);
                    node.children.remove(i);
                }
                lastChild = node.lastChild; // 1- put last child of the node to the this instance
                node.lastChild = children.get(0); // 2- last child of the node is the first children which moved excessive
                children.set(0, null); // 3- set that excessive pointer to null; first pointer of the new child is always null
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    void Split(int minDeg, BPlusTreeNode parent, BPlusTreeNode newchild, boolean rootSplit) {
        try {
            logger.debug("splitting the node {}. new sibling is {}", getId(), newchild);
            int mid = minDeg - 1; // just as a index help

            // 1- move items for this instance to newchild
            newchild.move(this, mid, 2 * mid); // TODO: moving by another object is confusing!

            // 2- add the new key in the parent node
            if (rootSplit) {
                parent.InsertNewRoot(newchild.keys.get(0), getId(), newchild.getId());
                parent.lowKey = lowKey;
                parent.highKey = highKey;
                parent.height = height + 1;
            } else
                parent.Insert(newchild.keys.get(0), newchild.getId());
            logger.debug("added the first key of new node {} int parent node {}", newchild.getId(), parent.getId());

//        if (parent.getSize() == 1) { // It means that it is a new root
//            parent.lowKey = lowKey;
//            parent.highKey = highKey;
//            parent.nodeHeight = nodeHeight + 1;
//        }

            // 3- add properties of the newly created node
            newchild.height = height;
            newchild.lowKey = newchild.keys.get(0);
            newchild.highKey = highKey;
            if (next.isSet)
                newchild.next.setLink(next.getLink());
            next.setLink(newchild.getId());
            highKey = newchild.keys.get(0) - 1;
            logger.debug("split completed. the node is changed to {}", toStringContent());
            logger.debug("split completed. The new sibling is {}", newchild.toStringContent());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    boolean InsertNewRoot(int key, ObjId oldRoot, ObjId newChild) {
        try {
            if (isLeaf) // this method should only be used for new root node
                return false;

            keys.add(key);
            children.add(oldRoot);
            lastChild = newChild;
            logger.debug("The key {} is inserted in the parent (new root) node {}. the new child is {}", key, getId(), newChild);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    /**
     * Insert a new key in an inner node with a key and an ObjId pointing to a new node created by split operation
     * @param key the key to inserted in the node
     * @param newchild the ObjId pointing to a new node created by the split operation
     * @return
     */
    boolean Insert(int key, ObjId newchild) {
        try {
            if (isLeaf) // this method should never be called for a leaf.
                return false;

            int i = 0;
            for (; i < keys.size() && key > keys.get(i); i++) // TODO: Is the position i correct?
                continue;
            if (i < keys.size())
                children.add(i + 1, newchild);
            else {
                children.add(i, lastChild);
                lastChild = newchild;
            }
            keys.add(i, key);
            logger.debug("The key {} is inserted in the parent node {}. The new child is {}", key, getId(), newchild);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    /**
     * Insert a new key in the node. Suppose that the server checks for the size before calling this method. So it is safe for inserting.
     * @param key the key to inserted in the node
     * @param value the value to inserted in the node
     * @return
     */
    boolean Insert(int key, int value) {
        try {
            if (!isLeaf) // this method should never be called for inner nodes.
                return false;

            int i = 0;
            for (; i < keys.size() && key > keys.get(i); i++) // TODO: Is the position i correct?
                continue;
            keys.add(i, key);
            values.add(i, value);
            logger.debug("the key {}, value {} is inserted in the leaf node {}", key, value, getId());
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    boolean Update(int key, int value) {
        try {
            logger.debug("updating the key {} with value {} in node {}", key, value, getId());
            NodeSearchValue val = Search(key);
            if (val.result.isAvailable) {
                int i = 0;
                for (; i < keys.size() && key > keys.get(i); i++)
                    continue;
                if (i < keys.size() && keys.get(i) == key)
                    values.set(i, value);
                logger.debug("updating the key {}, value {} in node {} was successful", key, value, getId());
                return true;
            }
            logger.debug("updating the key {}, value {} in node {} was not successful", key, value, getId());
            return false;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    @Override
    public Message getSuperDiff() {
        return new Message();
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
//        objectDiff.rewind();
    }

    @Override
    public String toString() {
        return "(" + this.getId() + ")=" + this.value;
    }
}
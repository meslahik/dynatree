package ch.usi.dslab.lel.dynastarv2.btree;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;

/**
 * Created by meslahik on 07.09.17.
 */

public class NodeSearchValue {
//    boolean isleaf;
    int nodeHeight;
    ObjId nodeId;
    ObjId childNodeId; // node id of the next child if the current node is not leaf

    Value result = new Value();

    public NodeSearchValue(ObjId nodeId) { // returning this means the node is a leaf but the value in not found
        this.nodeId = nodeId;
//        isleaf = true;
        nodeHeight = 1;
        this.result.isAvailable = false;
    }

    public NodeSearchValue(ObjId nodeId, Integer value) { // returning this means the node is a leaf and the value is found
        this.nodeId = nodeId;
//        isleaf = true;
        nodeHeight = 1;
        this.result.isAvailable = true;
        this.result.value = value;
    }

    public NodeSearchValue(ObjId nodeId, int nodeHeight, ObjId childNodeId) { // returning this means the node is not a leaf.
        this.nodeId = nodeId;
        this.nodeHeight = nodeHeight;
//        isleaf = false;
        this.childNodeId = childNodeId;
    }

    @Override
    public String toString() {
        return "(id=" + nodeId + ", height=" + nodeHeight + ", child id=" + childNodeId + ", result=" + result + ")";
    }
}

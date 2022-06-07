package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastarv2.probject.*;

/**
 * Created by meslahik on 01.06.17.
 */
public class BPlusTreeRoot extends PRObject {
//    ObjId objId;
    ObjId rootId;

    public BPlusTreeRoot() {

    }

    public BPlusTreeRoot(ObjId oid) {
        super(oid);
//        objId = oid;
        System.setErr(System.out);
    }

    void setRootId(ObjId objId) {
        rootId = objId;
    }

    ObjId getRootId() {
        return rootId;
    }

    @Override
    public String toString() {
        return "[id=" + getId() + ", root id=" + rootId + "]";
    }

    @Override
    public Message getSuperDiff() {
        return new Message();
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
//        objectDiff.rewind();
    }
}


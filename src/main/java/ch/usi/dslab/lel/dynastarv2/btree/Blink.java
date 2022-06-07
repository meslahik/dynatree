package ch.usi.dslab.lel.dynastarv2.btree;

import ch.usi.dslab.lel.dynastarv2.probject.ObjId;

/**
 * Created by meslahik on 14.08.18.
 */
public class Blink {
    private int link;
    boolean isSet = false;
    boolean isNull = true;

    public void setLink(ObjId link) {
        this.link = link.value;
        isSet = true;
        isNull = false;
    }

    public ObjId getLink() {
        return new ObjId(link);
    }
}

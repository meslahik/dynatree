package ch.usi.dslab.lel.dynastarv2.btree;

/**
 * Created by meslahik on 07.09.17.
 */
public class Value {
    boolean isAvailable = false;
    Integer value;

    public Value() {

    }

    @Override
    public String toString() {
        if (isAvailable)
            return "(" + value + ")" ;
        else
            return "()";
    }
}

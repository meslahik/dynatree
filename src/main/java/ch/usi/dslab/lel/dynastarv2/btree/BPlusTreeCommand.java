package ch.usi.dslab.lel.dynastarv2.btree;
import ch.usi.dslab.lel.dynastarv2.messages.MessageType;

/**
 * Created by meslahik on 07.09.17.
 */
public enum BPlusTreeCommand implements MessageType{
    /* In DynaStar, it is assumed that we are working items one by one.
     * In Btree items are packed in cells.
     * In DynaStar terminology, Read and Create operations are among the basic ones in GenericCommand. So, it is assumed
     *  that your application level commands are in AppCommand.
     * In BTree, search is just Read. Also, there is no Create. We insert something which is not in GenericCommand. it
     *  may create a DynaStar item (here a cell) or just add it to an exiting one.
     * */
    SEARCH, SEARCHFOUND, SEARCHNOTFOUND, SEARCHRETRY, INSERT, INSERTED, INSERTRETRY, RETRYSPLIT, INSERTSPLIT, SPLITRETRY, UPDATED
}

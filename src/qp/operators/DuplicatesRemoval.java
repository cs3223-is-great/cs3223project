/*
  To project out the required attributes from the result
 */

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.Vector;
import java.util.ArrayList;
public class DuplicatesRemoval extends Operator {

    private Operator base;
    private Vector attrSet;
    private int batchsize;  // number of tuples per outbatch
    private Operator sortedfile;
    private int numBuff;

    /**
     ** Required variables for DuplicatesRemoval operator execution 
     **/

    private boolean eos;  // boolean variable is true when end of stream is reached 
    private Batch inbatch;
    private int start;   // Cursor position in the input buffer


    private int[] attrIndex;


    public DuplicatesRemoval(Operator base, Vector ats, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrSet = ats;
        this.numBuff = numBuff;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }
    
    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public Vector getProjAttr() {
        return attrSet;
    }

    /**
     ** Opens the connections 
     **/

    public boolean open() {
        start = 0;
        eos = false;
        
        /* set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;


        /** The following loop finds out the index of the columns that
         ** are required from the base operator
         **/

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];

    	//System.out.println("base Schema---------------");
    	//Debug.PPrint(baseSchema);
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;
        }

        sortedfile = new Sort(base, attrSet, numBuff);
        sortedfile.setSchema(baseSchema);

        return sortedfile.open();
    }

    /**
     * Read next tuple from operator
     */

    public Batch next() {
        int i = 0;

        if (eos == true) {
            close();
            return null;
        }

        /* An output buffer is initiated**/
        Batch outbatch = new Batch(batchsize);


        /* keep on checking the incoming pages until
          the output buffer is full
         */
        Vector last = null;
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = sortedfile.next();
                /* There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            for (i = start; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.elementAt(i);
                Vector curr = new Vector();
                for (int j = 0; j < attrSet.size(); j++) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    curr.add(data);
                }
                if (curr.equals(last)) {
                    continue;
                }

                last = curr;
                Tuple outtuple = new Tuple(curr);
                outbatch.add(outtuple);

                if (outbatch.isFull()) {
                    if (i == inbatch.size()) {
                        start = 0;
                    } else {
                        start = i + 1;
                    }
                    return outbatch;
                }
            }
            start = 0;
        }
       return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        sortedfile.close();
        return true;

    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        DuplicatesRemoval newproj = new DuplicatesRemoval(newbase, newattr, optype, numBuff);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
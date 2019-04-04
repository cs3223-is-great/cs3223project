package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Vector;

public class SortMergeJoin extends Join {

    int batchsize;  //Number of tuples per out batch

    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    Batch leftbatch;
    Batch subLeftBatch;
    Batch rightB;
    
    ObjectOutputStream out;
    
    Operator leftS;
    Operator subLeftScaner;
    Operator rightS;
    
    int lcurs = -1;
    int sublcurs = -1;
    int rcurs = -1;
    
    Tuple lefttuple;
    
    int leftFirstIndex = -1;
    int rightFirstIndex = -1;
    int leftMidIndex = -1;
    int rightMidIndex = -1;
    
    boolean eos;  // Whether end of join
    boolean eols;  // Whether end of join
    
    boolean eoslb;  // Whether end of stream (left batch) is reached
    boolean eolb;  // Whether end of left batch is reached
    boolean eorb;  // Whether end of right batch is reached
    
    int mergeState = 0;

    static int count = 0;
    
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /**
     * During open finds the index of the join attributes
     * *  Materializes the right hand side into a file
     * *  Opens the connections
     **/


    public boolean open() {

        /* select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;

        Vector leftAs = new Vector();
        leftAs.add(leftattr);
        leftS = new Sort(left, leftAs, numBuff);
        leftS.setSchema(left.getSchema());
        
        Vector rightAs = new Vector();
        rightAs.add(rightattr);
        rightS = new Sort(right, rightAs, numBuff);
        rightS.setSchema(right.getSchema());
        
        if (!rightS.open() || !leftS.open()) {
            System.out.println("Error with opening the file");
            return false;
        }
        
        Batch inbatch;
        String leftTable = "LeftSortedTable";
     
        try {
            out = new ObjectOutputStream(new FileOutputStream(leftTable + ".tbl"));
            while ((inbatch = leftS.next()) != null) {
                for (int i = 0; i < inbatch.size(); i++) {
                    out.writeObject(inbatch.elementAt(i));
                }
            }
            leftS.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        leftS = new Scan(leftTable, OpType.SCAN);
        leftS.setSchema(left.getSchema());
        if (!leftS.open()) {
        	System.out.println("Error with opening the file");
            return false;
        }

        String rightTable = "RightSortedTable";
        try {
            out = new ObjectOutputStream(new FileOutputStream(rightTable + ".tbl"));
            while ((inbatch = rightS.next()) != null) {
                for (int i = 0; i < inbatch.size(); i++) {
                    out.writeObject(inbatch.elementAt(i));
                }
            }
            rightS.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rightS = new Scan(rightTable, OpType.SCAN);
        rightS.setSchema(right.getSchema());
        if (!rightS.open()) {
        	System.out.println("Error with opening the file");
            return false;
        }

        /* initialize the cursors of input buffers **/
        lcurs = 0;
        eolb = true;
        rcurs = 0;
        eorb = true;

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        int i, j;

        if (eos) {
            return null;
        }
        Batch outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            /* deal with read and end flag */
            if (lcurs == 0 && eolb == true) {
                leftbatch = leftS.next();
                if (leftbatch == null || leftbatch.isEmpty()) {
                    if (mergeState == 3) {
                        mergeState = 5;
                        try {
                            out.close();
                            count--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        rightFirstIndex = 0;
                        rcurs = 0;
                        eorb = true;
                        eols = true;
                    }
                    else {
                        eos = true;
                    }
                }
                eolb = false;
            }
            if (rcurs == 0 && eorb == true) {
                rightB = rightS.next();
                if (rightB == null || rightB.isEmpty()) {
                    eos = true;
                }
                eorb = false;
            }
            
            /* deal with compare and state transition */
            Tuple leftTuple = null;
            Tuple rightTuple = null;
            int cmpRst = 0;
            if (!eos) {
                leftTuple = leftbatch.elementAt(lcurs);
                rightTuple = rightB.elementAt(rcurs);
                cmpRst = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
                int leftattr = (int) leftTuple.dataAt(leftindex);
                int rightattr = (int) rightTuple.dataAt(rightindex);
            }
            switch (mergeState) {
                case 0:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        if (lcurs != leftbatch.size() - 1) {
                            lcurs++;
                        }
                        else {
                            lcurs = 0;
                            eolb = true;
                        }
                    }
                    else if (cmpRst > 0) {
                        if (rcurs != rightB.size() - 1) {
                            rcurs++;
                        }
                        else {
                            rcurs = 0;
                            eorb = true;
                        }
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftTuple.joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftFirstIndex = lcurs;
                        rightFirstIndex = rcurs;
                        lefttuple = leftTuple;
                        if (lcurs != leftbatch.size() - 1) {
                            mergeState = 1;
                            lcurs++;
                        }
                        else if (rcurs != rightB.size() - 1) {
                            mergeState = 2;
                            rcurs++;
                            leftMidIndex = leftFirstIndex;
                        }
                        else {
                            mergeState = 3;
                            Object joinAttr = leftTuple.dataAt(leftindex);
                            try {
                                out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                count++;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            flushCurrentLeftBatch(leftbatch, leftFirstIndex, leftbatch.size(), joinAttr);
                            lcurs = 0;
                            eolb = true;
                            rightMidIndex = rightFirstIndex;
                        }
                    }
                    break;
                case 1:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        System.out.println("Error in merge join!");
                        System.exit(1);
                    }
                    else if (cmpRst > 0) {
                        mergeState = 7;
                        leftMidIndex = leftFirstIndex;
                        if (rcurs != rightB.size() - 1) {
                            rcurs++;
                        } else {
                            rcurs = 0;
                            eorb = true;
                        }
                    }
                    else {
                        Tuple outtuple = leftTuple.joinWith(rightTuple);
                        outbatch.add(outtuple);
                        if (lcurs != leftbatch.size() - 1) {
                            lcurs++;
                        }
                        else if (rcurs != rightB.size() - 1) {
                            mergeState = 2;
                            rcurs++;
                            leftMidIndex = leftFirstIndex;
                        }
                        else {
                            mergeState = 3;
                            Object joinAttr = leftTuple.dataAt(leftindex);
                            try {
                                out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                count++;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            flushCurrentLeftBatch(leftbatch, leftFirstIndex, leftbatch.size(), joinAttr);
                            lcurs = 0;
                            eolb = true;
                            rightMidIndex = rightFirstIndex;
                        }
                    }
                    break;
                case 2:
                    if (eos) {
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        mergeState = 0;
                        if (lcurs != leftbatch.size() - 1) {
                            lcurs++;
                        }
                        else {
                            lcurs = 0;
                            eolb = true;
                        }
                    }
                    else if (cmpRst > 0) {
                        System.out.println("Error in merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftbatch.elementAt(leftMidIndex).joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftMidIndex++;
                        if (leftMidIndex == leftbatch.size()) {
                            if (rcurs != rightB.size() - 1) {
                                rcurs++;
                                leftMidIndex = leftFirstIndex;
                            }
                            else {
                                mergeState = 3;
                                Object joinAttr = leftTuple.dataAt(leftindex);
                                try {
                                    out = new ObjectOutputStream(new FileOutputStream("SMJtemp.tbl"));
                                    count++;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                flushCurrentLeftBatch(leftbatch, leftFirstIndex, leftbatch.size(), joinAttr);
                                lcurs = 0;
                                eolb = true;
                                leftMidIndex = -1;
                                rightMidIndex = rightFirstIndex;
                            }
                        }
                    }
                    break;
                case 3:
                    if (eos) {
                        try {
                            out.close();
                            count--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return outbatch;
                    }
                    if (cmpRst < 0) {
                        System.out.println("Error in merge join!");
                        System.exit(1);
                    }
                    else if (cmpRst > 0) {
                        mergeState = 5;
                        Object joinAttr = leftTuple.dataAt(leftindex);
                        flushCurrentLeftBatch(leftbatch, 0, lcurs, joinAttr);
                        try {
                            out.close();
                            count--;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        rightFirstIndex = 0;
                        rcurs = 0;
                        eorb = true;
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftTuple.joinWith(rightB.elementAt(rightMidIndex));
                        outbatch.add(outtuple);
                        rightMidIndex++;
                        if (rightMidIndex == rightB.size()) {
                            rightMidIndex = rightFirstIndex;
                            if (lcurs != leftbatch.size() - 1) {
                                lcurs++;
                            } else {
                                mergeState = 3;
                                Object joinAttr = leftTuple.dataAt(leftindex);
                                flushCurrentLeftBatch(leftbatch, 0, leftbatch.size(), joinAttr);
                                lcurs = 0;
                                eolb = true;
                            }
                        }
                    }
                    break;
                case 4:
                    if (eos) {
                        subLeftScaner.close();
                        return outbatch;
                    }
                    if (sublcurs == 0 && eoslb) {
                        subLeftBatch = subLeftScaner.next();
                        if (subLeftBatch == null || subLeftBatch.isEmpty()) {
                            mergeState = 5;
                            subLeftScaner.close();
                            rightFirstIndex = 0;
                            rcurs = 0;
                            eorb = true;
                            break;
                        }
                        else {
                            eoslb = false;
                            rightMidIndex = rightFirstIndex;
                        }
                    }
                    for (i = sublcurs; i < subLeftBatch.size(); i++) {
                        for (j = rightMidIndex; j < rightB.size(); j++) {
                            Tuple lefttuple = subLeftBatch.elementAt(i);
                            Tuple righttuple = rightB.elementAt(j);
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == subLeftBatch.size() - 1 && j == rightB.size() - 1) {//case 1
                                    sublcurs = 0;
                                    eoslb = true;
                                    rightMidIndex = rightFirstIndex;
                                } else if (i != subLeftBatch.size() - 1 && j == rightB.size() - 1) {//case 2
                                    sublcurs = i + 1;
                                    rightMidIndex = rightFirstIndex;
                                } else {
                                    sublcurs = i;
                                    rightMidIndex = j + 1;
                                }
                                return outbatch;
                            }
                        }
                        rightMidIndex = rightFirstIndex;
                    }
                    sublcurs = 0;
                    eoslb = true;
                    break;
                case 5:
                    if (eos) {
                        return outbatch;
                    }
                    cmpRst = Tuple.compareTuples(lefttuple, rightTuple, leftindex, rightindex);
                    if (cmpRst < 0) {
                        if (eols) {
                            eos = true;
                        }
                        mergeState = 6;
                        subLeftScaner = new Scan("SMJtemp", OpType.SCAN);
                        subLeftScaner.setSchema(left.getSchema());
                        if (!subLeftScaner.open()) {
                        	System.out.println("Error with opening the file");
                            System.exit(2);
                        }
                        sublcurs = 0;
                        eoslb = true;
                        rightMidIndex = 0;
                        assert rightFirstIndex == 0;
                    }
                    else if (cmpRst > 0) {
                        System.out.println("Error in merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        if (rcurs != rightB.size() - 1) {
                            rcurs++;
                        }
                        else {
                            mergeState = 4;
                            subLeftScaner = new Scan("SMJtemp", OpType.SCAN);
                            subLeftScaner.setSchema(left.getSchema());
                            if (!subLeftScaner.open()) {
                            	System.out.println("Error with opening the file");
                                System.exit(2);
                            }
                            sublcurs = 0;
                            eoslb = true;
                            rightMidIndex = rightFirstIndex;
                            assert rightFirstIndex==0;
                        }
                    }
                    break;
                case 6:    
                    if (eos) {
                        subLeftScaner.close();
                        return outbatch;
                    }
                    if (sublcurs == 0 && eoslb) {
                        subLeftBatch = subLeftScaner.next();
                        if (subLeftBatch == null || subLeftBatch.isEmpty()) {
                            mergeState = 0;
                            subLeftScaner.close();
                            break;
                        }
                        else {
                            eoslb = false;
                            rightMidIndex = 0;
                        }
                    }
                    for (i = sublcurs; i < subLeftBatch.size(); i++) {
                        for (j = rightMidIndex; j < rcurs; j++) {
                            Tuple lefttuple = subLeftBatch.elementAt(i);
                            Tuple righttuple = rightB.elementAt(j);
                            Tuple outtuple = lefttuple.joinWith(righttuple);

                            outbatch.add(outtuple);
                            if (outbatch.isFull()) {
                                if (i == subLeftBatch.size() - 1 && j == rcurs) {//case 1
                                    sublcurs = 0;
                                    eoslb = true;
                                    rightMidIndex = 0;
                                } else if (i != subLeftBatch.size() - 1 && j == rcurs) {//case 2
                                    sublcurs = i + 1;
                                    rightMidIndex = 0;
                                } else {
                                    sublcurs = i;
                                    rightMidIndex = j + 1;
                                }
                                return outbatch;
                            }
                        }
                        rightMidIndex = 0;
                    }
                    sublcurs = 0;
                    eoslb = true;
                    break;
                case 7:
                    if (eos) {
                        return outbatch;
                    }
                    cmpRst = Tuple.compareTuples(lefttuple, rightTuple, leftindex, rightindex);
                    if (cmpRst < 0) {
                        mergeState = 0;
                    }
                    else if (cmpRst > 0) {
                        System.out.println("Error in merge join!");
                        System.exit(1);
                    }
                    else {// cmpRst == 0
                        Tuple outtuple = leftbatch.elementAt(leftMidIndex).joinWith(rightTuple);
                        outbatch.add(outtuple);
                        leftMidIndex++;
                        if (leftMidIndex == leftbatch.size() || leftMidIndex == lcurs) {
                            leftMidIndex = leftFirstIndex;
                            if (rcurs != rightB.size() - 1) {
                                rcurs++;
                            }
                            else {
                                rcurs = 0;
                                eorb = true;
                            }
                        }
                    }
                    break;
            }
        }
        return outbatch;
    }

    private void flushCurrentLeftBatch(Batch leftBatch, int leftTupleFrom, int leftTupleTo, Object joinAttr) {
        try {
            for (int i = leftTupleFrom; i < leftTupleTo; i++) {
                out.writeObject(leftBatch.elementAt(i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Close the operator
     */
    public boolean close() {
        leftS.close();
        rightS.close();
        
        boolean f1, f2, f3;
        File f = new File("SMJtemp.tbl");
        f1 = f.delete(); 
        f = new File("RightSortedTable.tbl");
        f3 = f.delete();
        f = new File("LeftSortedTable.tbl");
        f2 = f.delete();
        if (!f2 || !f3) {
            return false;
        }
        return (f2 && f3);

    }


}











































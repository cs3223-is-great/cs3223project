/* Sort in memory Operation **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class Sort extends Operator {

    private Operator base;  // base operator
    private Vector attrSet;  //Order by
    private int batchsize;  // number of tuples per outbatch

    private int[] attrIndex;
    private int numBuff;
    int numInputBuff;
    /**
     * The following fields are required during
     * * execution of the Sort operator
     **/
    private static int NUMINSTANCES = 0;

    private int instanceNumber;
    private int numPass = 0;
    private int numRun = 0;
    private boolean isExtSort = true;  // boolean variable is true for external sort
    private boolean eos;  // Indiacate whether end of stream is reached or not
    private List<Tuple> buffer; // main memory
    private Batch[] inbatches; //main memory
    private Scan[] runScanner; // write in tuples, read in batches
    private int[] tupleIndexes; // default value is zero
    private int start = 0;


    /**
     * constructor
     **/

    public Sort(Operator base, Vector as, int numBuff) {
        super(OpType.SORT);
        this.base = base;
        this.attrSet = as;
        this.numBuff = numBuff;
        this.numInputBuff = numBuff - 1;
        this.instanceNumber = NUMINSTANCES++;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    /**
     * Opens the connection to the base operator
     **/

    public boolean open() {
        eos = false;    

		/** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        attrIndex = new int[attrSet.size()];
        
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = schema.indexOf(attr);
            attrIndex[i] = index;
        }

        if (base.open()) {
            buffer = new ArrayList<Tuple>(numBuff * batchsize); 
            numPass = 0;
            numRun = 0;
            while (true) { 
                int batchIndex;
                
                /* read the tuples in buffer as much as possible */
                for (batchIndex = 0; batchIndex < numBuff; batchIndex++) {
                    Batch inbatch = base.next();
                    if (inbatch == null || inbatch.size() == 0) {
                        if (numRun == 0) {
                            isExtSort = false;
                        }
                        break;
                    }
                    for (int i = 0; i < inbatch.size(); i++) {
                        buffer.add(inbatch.elementAt(i));
                    }
                }
                /* sort the data in buffer */
                buffer.sort((arg0, arg1) -> {
                    for (int anAttrIndex : attrIndex) {
                        int cmpresult = Tuple.compareTuples(arg0, arg1, anAttrIndex);
                        if (cmpresult != 0) {
                            return cmpresult;
                        }
                    }
                    return 0;
                });

                if (isExtSort == true) {
                    if (numBuff <= 2) {
                        System.out.println("Minimum 3 buffers are required for an external sort operator");
                        System.exit(1);
                    }
                    try {
                        String pname = "Run-" + instanceNumber + "-" + numPass + "-" + numRun + ".tbl";
                        File runFile = new File(pname);
                        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(runFile));

                        for (Tuple tuple : buffer) {;
                            out.writeObject(tuple);
                        }
                        out.close();
                    } catch (IOException e) {
                        System.out.println("ExternalSort: Error in writing the temporary file");
                    }
                    numRun++;
                    buffer.clear();

                    if (batchIndex != numBuff) {
                        break;
                    }
                }

                else {
                    start = 0;
                    boolean isClose = base.close();
                    return isClose;
                }
            }

            inbatches = new Batch[numInputBuff];
            runScanner = new Scan[numInputBuff];
            tupleIndexes = new int[numInputBuff];

            while (numRun > numInputBuff) {
                int readPass = numPass;
                numPass++;
                int numMerge = (numRun + numInputBuff - 1) / numInputBuff;
                int mergeFrom = 0;
                for (int mergeIndex = 0; mergeIndex < numMerge; mergeIndex++) {
                    int numMergeRuns = Math.min(numInputBuff, numRun - mergeFrom);
                    openRuns(schema, instanceNumber, readPass, mergeFrom, numMergeRuns);
                    mergeRuns(instanceNumber, numPass, mergeIndex, numMergeRuns, attrIndex, batchsize, false);
                    mergeFrom += numInputBuff;
                }
                numRun = numMerge;
            }
            /* leave the runs belong to the last pass open */
            openRuns(schema, instanceNumber, numPass, 0, numRun);
            return base.close();
        } else
            return false;
    }

    private void openRuns(Schema schema, int instanceNumber, int numPasses, int mergeFrom, int numMergeRuns) {
        /* read each first batch of sorted files into memory */
        for (int runIndex = 0; runIndex < numMergeRuns; runIndex++) {
            int runNumber = mergeFrom + runIndex;
            String runName = "Run-" + instanceNumber + "-" + numPasses + "-" + runNumber;
            runScanner[runIndex] = new Scan(runName, OpType.SCAN);
            runScanner[runIndex].setSchema(schema);
            if (!runScanner[runIndex].open()) {
                System.out.println("@@@@Oops! open error!");
            }
            Batch inbatch = runScanner[runIndex].next();
            if (inbatch == null || inbatch.size() == 0) {
                tupleIndexes[runIndex] = -1;
            }
            else {
                inbatches[runIndex] = inbatch;
                tupleIndexes[runIndex] = 0;
            }
        }
    }

    private Batch mergeRuns(int instanceNumber, int numPasses, int mergeIndex, int numMergeRuns, int[] attrIndex, int batchsize, boolean isLastMerge) {
        Batch outbatch = new Batch(batchsize);
        int minBatch = -1;
        Tuple minTuple = null;
        boolean onlyOneLeft = false; 
        ObjectOutputStream out = null;

        if (!isLastMerge) {
            String pathname = "Run-" + instanceNumber + "-" + numPasses + "-" + mergeIndex + ".tbl";
            File runFile = new File(pathname);
            try {
                out = new ObjectOutputStream(new FileOutputStream(runFile));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        do {
            while (!outbatch.isFull()) {
                if (!onlyOneLeft) {
                    /* Looking for the next tuple and its batch number */
                    onlyOneLeft = true;
                    for (int i = 0; i < numMergeRuns; i++) {
                        if (tupleIndexes[i] != -1) {
                            Tuple currTuple = inbatches[i].elementAt(tupleIndexes[i]);
                            if (minTuple == null) {
                                minTuple = currTuple;
                                minBatch = i;
                            } else {
                                onlyOneLeft = false; 
                                for (int j : attrIndex) {
                                    int diff = Tuple.compareTuples(minTuple, currTuple, j);
                                    if (diff > 0) {
                                        minBatch = i;
                                        minTuple = currTuple;
                                        break;
                                    } else if (diff < 0) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                } else {
                    minTuple = inbatches[minBatch].elementAt(tupleIndexes[minBatch]);
                }

         
                if (isLastMerge) { // if it is the last merge, add the tuple to output
                    outbatch.add(minTuple); 
                } else { 
                    try {
                        assert out != null;
                        out.writeObject(minTuple);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                minTuple = null;

                /* update tupleIndexes */
                if (tupleIndexes[minBatch] == inbatches[minBatch].size() - 1) {
                    Batch inbatch = runScanner[minBatch].next();
                    if (inbatch == null || inbatch.size() == 0) {
                        tupleIndexes[minBatch] = -1;
                        runScanner[minBatch].close();
                        File f = new File(runScanner[minBatch].getTabName() + ".tbl");
                        f.delete();
                        if (onlyOneLeft) {
                            if (isLastMerge) {
                                eos = true;
                                return outbatch;
                            } else {
                                try {
                                    out.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                return null;
                            }
                        }
                    }
                    else {
                        tupleIndexes[minBatch] = 0;
                        inbatches[minBatch] = inbatch;
                    }
                } else {
                    tupleIndexes[minBatch]++;
                }
            }
        } while (!isLastMerge);
        return outbatch;
    }


    public Batch next() {
        //System.out.println("Sort:-----------------in next--------------");
    	//Debug.PPrint(con);
    	//System.out.println();
        if (eos) {
            close();
            return null;
        }

        /* An output buffer is initiated**/
        Batch outbatch = new Batch(batchsize);
        int mergeIndex = 0;
        if (isExtSort) {
            outbatch = mergeRuns(instanceNumber, numPass, mergeIndex, numRun, attrIndex, batchsize, true);
            return outbatch;
        } else {
            while (!outbatch.isFull()) {
                /* There is no more incoming pages from base operator **/
                if (start == buffer.size()) {
                    eos = true;
                    return outbatch;
                }
                outbatch.add(buffer.get(start));
                start++;
            }
            return outbatch;
        }
    }


    /**
     ** closes the output connection
     **/

    public boolean close() {
        return true;
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        Sort newsel = new Sort(newbase, newattr, optype);
        newsel.setSchema(newbase.getSchema());
        return newsel;
    }
}


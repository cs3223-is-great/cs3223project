package qp.operators;

import java.util.*;

import qp.utils.*;

public class SortMergeJoin extends Join {

	int batchsize; // Number of tuples per out batch

	/**
	 * The following fields are useful during execution of the NestedJoin
	 * operation
	 **/
	int leftindex; // Index of the join attribute in left table
	int rightindex; // Index of the join attribute in right table

	Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
	List<Tuple> ofTuples; // store the overflow tuples here

	int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer

	Vector<Tuple> tempRightEqualTuples;
	Vector<Tuple> tempLeftEqualTuples;
	
	static String RIGHT="right";
	static String LEFT="left";

	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}
	
	public boolean open() {
		
		if (!left.open()||!right.open()){
			return false;
		}
		
		int tuplesize=schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;
        
        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

		leftbatch = left.next();
		rightbatch = right.next();

		tempRightEqualTuples = new Vector<Tuple>();
		tempLeftEqualTuples = new Vector<Tuple>();

		ofTuples = new ArrayList<Tuple>();

		return true;
	}

	public Batch next() {
		
		//finish join
		if (leftbatch == null || rightbatch == null) {
			return null;
		}
		
		outbatch = new Batch(batchsize);

		//add last times overfolwTuples back to the outbatch 
		while (!outbatch.isFull() && !ofTuples.isEmpty()) {
			outbatch.add(ofTuples.get(0));
			ofTuples.remove(0);
		}

		//do the join
		while (!outbatch.isFull() && !(leftbatch == null || rightbatch == null)) {
			
			//compare two tuples
			int compareResult = Tuple.compareTuples(leftbatch.elementAt(lcurs), rightbatch.elementAt(rcurs), leftindex, rightindex);
			
			if (compareResult == 0) {//matches join
				tempLeftEqualTuples.add(leftbatch.elementAt(lcurs));
				tempRightEqualTuples.add(rightbatch.elementAt(rcurs));

				//search for tuples matches join on the right file, store matched tuples into tempRightEqualTuples
				rcurs = updateCurs(rcurs,rightbatch,RIGHT);
				while (rcurs != -1 && Tuple.compareTuples(tempLeftEqualTuples.get(0), rightbatch.elementAt(rcurs),leftindex, rightindex) == 0) {
					tempRightEqualTuples.add(rightbatch.elementAt(rcurs));
					rcurs = updateCurs(rcurs,rightbatch,RIGHT);
				}

				//search for tuples matches join on the left file, store matched tuples into tempLeftEqualTuples 
				lcurs = updateCurs(lcurs,leftbatch,LEFT);
				while (lcurs != -1 && Tuple.compareTuples(leftbatch.elementAt(lcurs), tempRightEqualTuples.get(0),leftindex, rightindex) == 0) {
					tempLeftEqualTuples.add(leftbatch.elementAt(lcurs));
					lcurs = updateCurs(lcurs,leftbatch,LEFT);
				}

				//load matched tuple into outbatch, if outbatch if full, then put the matched tuple into tempEqualTuples
				for (Tuple leftTuple : tempLeftEqualTuples) {
					for (Tuple rightTuple : tempRightEqualTuples) {
						if (!outbatch.isFull()) {
							outbatch.add(leftTuple.joinWith(rightTuple));
						} else {
							ofTuples.add(leftTuple.joinWith(rightTuple));
						}
					}
				}
				tempRightEqualTuples = new Vector<Tuple>();
				tempLeftEqualTuples = new Vector<Tuple>();

			} else if (compareResult < 0) {//the value in the left current tuple is smaller than the right ones
				lcurs = updateCurs(lcurs,leftbatch,LEFT);
			} else if (compareResult > 0) {//the value in the right current tuple is smaller than the left ones
				rcurs = updateCurs(rcurs,rightbatch,RIGHT);
			}
		}
		return outbatch;
	}
	
	//update the curs to the right position
    private int updateCurs(int curs, Batch batch, String side) {
    	
    	if(batch==null){//end of file, can finish join
    		return -1;
    	} else {
    		if(curs!=(batch.capacity()-1)){//does not reach the end of batch
    			return curs+1;
    		} else {
    			if(side.equals("left")){//reached the end of left batch, load a new batch and point to the beginning of the batch
    				batch=left.next();
        			return 0;
    			} else {//reached the end of right batch, load a new batch and point to the beginning of the batch
    				batch=right.next();
        			return 0;
    			}
    		}
    	}
	}

	public boolean close() {
	if (left.close() && right.close()) {
	    return true;
	} else
	    return false;
    }
}

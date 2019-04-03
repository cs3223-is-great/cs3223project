/** index nested loop join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;

public class IndexNested extends Join{


    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the IndexNested operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    HashIndex rTable; // HashIndex built from the right table

    Vector<Tuple> rTuples; //search for tuples from the right table
    
    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    

    int lcurs;    // Cursor for left side buffer
    int rcurs;    //Cursor for right side matching records
    boolean eosl;  // Whether end of stream (left table) is reached

    public IndexNested(Join jn){
	    super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
	    schema = jn.getSchema();
	    jointype = jn.getJoinType();
	    numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     ** Loads the right hand side into a hash index
     ** Opens the connections
     **/
    
    
    
    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize = Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
            
        /** initialize the cursors of input buffers **/
        lcurs = 0;
        eosl = false;
            
        /** right hand side table will be loaded into HashIndex **/
        
        if(!right.open()){
            return false;
        } else{ 
        	
            rTable = new HashIndex(); //initiate the hash index for right table
            rcurs = 0;
            rTable.setAttribute(rightindex);
            
            
            while((rightpage = right.next()) != null){ 
                rTable.load(rightpage); //load pages from rTable into hash index
            }
            rTuples = new Vector<Tuple>();  //add tuples into a vector from the right table
        }
        if(left.open())
            return true;
        else
            return false;
    }


    /** from input tables selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/
    
    
    public Batch next(){
	//System.out.print("NestedJoin:--------------------------in next----------------");
	//Debug.PPrint(con);
	//System.out.println();
	//int i,j;
    if(eosl){
    	if(lcurs == 0){
    		close();
            return null;
    	}
        
    }
    outbatch = new Batch(batchsize);
 
    while (!outbatch.isFull()){      
    	
    	/** unchecked tuples from right table**/
        if (rcurs != 0) {
        	
            Tuple currLeftTuple = leftbatch.elementAt(lcurs); 
            for (int i = rcurs; i < rTuples.size(); i++) { //process previously found matching tuples and add them to output
                Tuple righttuple = rTuples.elementAt(i);
                if (currLeftTuple.checkJoin(righttuple,leftindex,rightindex)) {
                    Tuple outtuple = currLeftTuple.joinWith(rTuples.elementAt(i));
                    outbatch.add(outtuple);                 
                    if(outbatch.isFull()){
                        if(i == rTuples.size()-1){//case 1:  all matching tuples added
                            rcurs=0;
                            lcurs = (lcurs + 1) % leftbatch.size();
                        } else { //case 2: some matching tuples in right table are still not added due to insufficient space in outbatch
                            rcurs = i+1;
                        }
                        return outbatch;
                    }
                }                  
            }
            //reset rcurs and increment lcurs when all previously found matching tuples from right table are fully processed
            rcurs = 0;
            lcurs = (lcurs + 1) % leftbatch.size();
        }
        
        if(lcurs==0){
            /** new left page is to be fetched**/
            leftbatch =(Batch) left.next();
            if(leftbatch==null) {
                eosl=true;
                return outbatch;
            }
        }
        
        for (int i = lcurs; i < leftbatch.size(); i++) {
            Tuple lefttuple = leftbatch.elementAt(i);
            
            /**retrieve from hash index tuples that can be joined with the current left tuple**/
            rTuples = rTable.get(lefttuple.dataAt(leftindex));
            if (rTuples != null && !rTuples.isEmpty()) {
                /** join current left tuple with each matching right tuple**/
                for (int j = rcurs; j < rTuples.size(); j++) {
                    Tuple righttuple = rTuples.elementAt(j);
                    if(lefttuple.checkJoin(righttuple,leftindex,rightindex)) {
                        Tuple outtuple = lefttuple.joinWith(rTuples.elementAt(j));
                        outbatch.add(outtuple);
                        
                        if(outbatch.isFull()){
                            if(j == rTuples.size()-1){//case 1: all matching tuples added
                                lcurs= (i+1) % leftbatch.size();
                                rcurs=0;
                            } else { //case 2: some matching tuples in right table not added 
                                lcurs = i;
                                rcurs = j+1;
                            }
                            return outbatch;
                        }
                    }
                }          
            }    
            rcurs = 0; //reset rcurs after all matching right tuples are processed
        }
        lcurs = 0; //reset lcurs after all left tuples in buffer are processed
    }
    return outbatch;
    }



    /** Close the operator */
    public boolean close(){    
        return left.close() && right.close();
    }
}
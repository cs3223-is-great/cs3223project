package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class HashJoin extends Join{

	int batchsize;  //Number of tuples per out batch
    int leftindex;  // Index of the join attribute in left table
    int rightindex; // Index of the join attribute in right table
    
	int rightBatchSize;
	int leftBatchSize;

    String rfname;    // The file name where the right table is materialize
    String temp_lfname;
    String temp_rfname;

    Batch outbatch;   // Output buffer
    Batch inputbatch_left;  // Buffer for input stream for left table
    Batch inputbatch_right; // Buffer for input stream for right table
    Batch[] in_memory; // B-2 >= size of largest partition held in memory

    ObjectInputStream rightInput; // File pointer to the right hand materialized file
    ObjectInputStream leftInput;  // File pointer to the left hand materialized file
    int currPIndex = -1; // index for the current partition

    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    
    boolean eosl = true;  // boolean variable is true when end of stream (left table) is reached
    boolean eosr = true;  // boolean variable is true when end of stream (right table) is reached

    public HashJoin(Join jn){
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     ** Finish the partition phase: partition right and left table into numBuff - 1 partitions
     **  
     **/



    public boolean open(){
		int tuplesize=schema.getTupleSize();
		//no. of tuples per batch for result table
		batchsize=Batch.getPageSize()/tuplesize;
		
		int rightTupleSize = right.getSchema().getTupleSize();
		//no. of tuples per batch for right table
		rightBatchSize = Batch.getPageSize()/rightTupleSize;
		
		int leftTupleSize = right.getSchema().getTupleSize();
		//no. of tuples per batch for left table
		leftBatchSize = Batch.getPageSize()/leftTupleSize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		
		/** initialize the cursors of input buffers **/
		lcurs = 0; rcurs =0;

		Batch inputpage;
		
		// initialise bucket with size of B-1 
		Batch[] buckets = new Batch[numBuff - 1];
		
		/*right table to be partitioned */	
		if (right.open()) {
		    try{
		    	//initialise bucket
		    	for(int i = 0; i < numBuff-1; i++){
		    		buckets[i] = new Batch(rightBatchSize);
		    	}
		    	
		    	// initialise the output of right table into a file name
		    	ObjectOutputStream[] rightOutput = new ObjectOutputStream[numBuff - 1];
		    	for(int i = 0; i<numBuff-1; i++){
		    	    String tempfname =  "HJtempRight-" + String.valueOf(i) + this.hashCode();
		    	    rightOutput[i] = new ObjectOutputStream(new FileOutputStream(tempfname));
		    	}
		    	
		    	// hash every right table page until the end of right table is reached
		    	// when each bucket is full, write into rightOutput
		    	while((inputpage = right.next()) != null){
		    		for(int i = 0; i < inputpage.size();i++){
		    			//hash each tuple in input right page
		    			Tuple tp = inputpage.elementAt(i);
		    			int key = Integer.valueOf(String.valueOf(tp.dataAt(rightindex)))%(numBuff-1);
		    			if(buckets[key].size() == rightBatchSize){
		    				rightOutput[key].writeObject(buckets[key]);
		    				buckets[key] = new Batch(rightBatchSize);
		    			}
		    			buckets[key].add(tp);
		    		}
		    	}
		    	
		    	//write the other buckets into rightOutput
		    	for(int i = 0; i < numBuff - 1; i++){
		    		if(!buckets[i].isEmpty()){
		    			rightOutput[i].writeObject(buckets[i]);
		    		}
		    	}
		    	
		    	// close rightOutput stream for each bucket
		    	for(int i = 0; i < numBuff -1 ;i++){
		    		rightOutput[i].close();
		    	}
		    	}catch(IOException io){
			    	System.out.println("HashJoin:writing the temporay file error for partition phase");
			    	return false;
		    	}

		    if(!right.close())
		    	return false;
		} else {
			return false;
		}
		
		/*partition left table*/
		if(!left.open())
		    return false;
		else{
		    try{
		    	//initialise bucket with the batch of leftBatchSize
		    	for(int i = 0; i < numBuff-1;i++){
		    		buckets[i] = new Batch(leftBatchSize);
		    	}
		    	// initialise the output of left table into a file name
		    	ObjectOutputStream[] leftOutput = new ObjectOutputStream[numBuff - 1];
		    	for(int i = 0; i<numBuff-1;i++){
		    		String tempfname = "HJtempLeft-" + String.valueOf(i)+ this.hashCode();
		    		leftOutput[i] = new ObjectOutputStream(new FileOutputStream(tempfname));
		    	}
		    	
		    	// hash every left table page until the end of left table is reached
		    	// when each bucket is full, write into leftOutput
		    	while((inputpage = left.next()) != null){
		    		for(int i = 0; i < inputpage.size();i++){
		    			Tuple tp = inputpage.elementAt(i);
		    			int key = Integer.valueOf(String.valueOf(tp.dataAt(leftindex)))%(numBuff-1);
		    			if(buckets[key].size() == leftBatchSize){
		    				leftOutput[key].writeObject(buckets[key]);
		    				buckets[key] = new Batch(leftBatchSize);
		    			}
		    			buckets[key].add(tp);
		    		}
		    	}
		    	//write the other buckets into leftOutput
		    	for(int i = 0; i < numBuff-1;i++){
		    		if(!buckets[i].isEmpty()){
		    			leftOutput[i].writeObject(buckets[i]);
		    		}
		    	}
	    	
		    	// close leftOutput stream for each bucket
		    	for(int i = 0; i < numBuff -1 ;i++){
		    		leftOutput[i].close();
		    	}
		    }catch(IOException io){
		    	System.out.println("HashJoin:writing the temporay file error for partition phase");
		    	return false;
		    }
		//}
		    if(!left.close())
		    	return false;
		}  
		rcurs = 0;
		lcurs = 0;
		return true;
    }



    /** construct in memory hash table for each partition of left table
     * each time fetch one page of right table from same partition
     * hash each tuple and match with the left table tuples in the same bucket
     * selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
		outbatch = new Batch(batchsize);
		
		while(!outbatch.isFull()){
			if(lcurs == 0 && rcurs == 0 && eosr == true){
					if(currPIndex == (numBuff-2) && eosl == true){
						// once all partitions are done, do the join
						if(!outbatch.isEmpty()) return outbatch;
						return null;
					}else{
						if(eosl == true){
							/** once the end of the current partition of the left table is reached,
							 ** start a new partition 
							 **/
							currPIndex++;
						    temp_rfname = "HJtempRight-" + String.valueOf(currPIndex)+ this.hashCode();
						    temp_lfname = "HJtempLeft-" + String.valueOf(currPIndex) + this.hashCode(); 
						}
 
					    try{
						    leftInput = new ObjectInputStream(new FileInputStream(temp_lfname));
						    rightInput = new ObjectInputStream(new FileInputStream(temp_rfname));
					    }catch(IOException io){
					    	System.out.println("HashJoin:error in opening outstream for join phase" + "in_lfname " + temp_lfname + " in_rfname " + temp_rfname);
					    	continue;
					    }
					    // initialize in-memory hashtable 
					    in_memory = new Batch[numBuff - 2];
					    for(int i = 0; i < numBuff - 2; i ++){
					    	in_memory[i] = new Batch(leftBatchSize);
					    }
					    eosl = false;
					    eosr = false;
					    inputbatch_right = null;
					    try{
					    	try{
					    		inputbatch_left = (Batch)leftInput.readObject();
					    		while(inputbatch_left == null) inputbatch_left = (Batch)leftInput.readObject();
					    	}catch(IOException io){
					    		//end of left table current partition
					    		eosr = true;
					    		eosl = true;
					    		rightInput.close();
					    		leftInput.close();
					    		continue;
					    	}
					    	
					        int key;
					        int inputindex = 0;
	
					        //hash a new left table partition into in-memory hashtable
					        while(true){
						        if(inputindex >= inputbatch_left.size()){
						        	try{
						        		inputbatch_left = (Batch)leftInput.readObject();
						        		while(inputbatch_left == null || inputbatch_left.isEmpty()) 
						        		   inputbatch_left = (Batch)leftInput.readObject();
						        		inputindex = 0;
						        	}catch(IOException io){
						        		leftInput.close();
						        		eosl = true;
						        		break;
						        	}	
						        }
						       
						        key = Integer.valueOf(String.valueOf(inputbatch_left.elementAt(inputindex).dataAt(leftindex)))%(numBuff-2);
	
						        //check whether the bucket is full; if yes, stop reading, matching right table tuples first,store rest of non-partitioned left table tuples in original file
						        if(in_memory[key].size() >= leftBatchSize){
						        	eosl = false;
						        	String tempFile = "tempFile" + this.hashCode();
						        	ObjectOutputStream temp = new ObjectOutputStream(new FileOutputStream(tempFile));
						        	//write the rest tuples in current input buffer back
						        	if(inputindex <= inputbatch_left.size() - 1){
						        		for(int i = 0; i < inputindex ; i++){
						        			inputbatch_left.remove(0);
						        		}
						        		temp.writeObject(inputbatch_left);
						        	}
						        	inputindex = 0;
						        	try{
						        		//write the rest left table pages into file
						        		while(true){
						        			temp.writeObject(leftInput.readObject());
						        		}
						        	}catch(IOException e){
						        		//reach end of left table partition	
						        		leftInput.close();
						        		temp.close();
						        		File f = new File(temp_lfname);
						        		f.delete();	
						        	}
						        	//write the rest left table pages in temp file back to original left table partition file
						        	ObjectOutputStream out_left = new ObjectOutputStream(new FileOutputStream(temp_lfname));
						        	ObjectInputStream temp_in = new ObjectInputStream(new FileInputStream(tempFile));
						        	try{
						        		while(true){
						        			out_left.writeObject(temp_in.readObject());
						        		}
						        	}catch(IOException io){
						        		//write finish
						        		try{
						        			temp_in.close();
						        			out_left.close();
							        		File f = new File(tempFile);
							        		f.delete();
							        		leftInput.close();
						        		}catch(IOException io1){
						        			System.out.println("Hashjoin: error in closing temp write fil " + temp_lfname);
						        		}
						        	}
						        	break;
						        }//finish writting the rest of the current partition back
	
						        //the bucket is not full, still able to write into the bucket
						        in_memory[key].add(inputbatch_left.elementAt(inputindex++));				        
					        }//finish hashing one left table partition into in-memory ht
	
					        //load the right table page for matching
					        try{		
					        	inputbatch_right = (Batch)rightInput.readObject();//handle the case if the partition is empty
					        	while(inputbatch_right == null || inputbatch_right.size() == 0) {
					        		inputbatch_right = (Batch)rightInput.readObject();
					        	}
					        	eosr = false;
					        }catch(IOException io){
					        	//end of the right table
					        	eosr = true;
					        	try{
					        		rightInput.close();
					        	}catch(IOException io1){
					        		System.out.println("HashJoin:error in closingfile");
					        	}
					        	continue;//start a new round 
					        }
					        	
					    }catch(IOException io){
					        System.out.println("HashJoin:end of input file");
					        eosl = true;
					        eosr = true;
					        continue;
					    } catch (ClassNotFoundException e) {
							System.out.println("HashJoin:Some error in deserialization ");
							System.exit(1);
						}  		    
					}
				}//finish starting a new round
	
			//generate result tuple
		    Tuple lefttuple;
		    Tuple righttuple;
		    int key;
		    Batch currBucket;
	
		    //hash each right table tuple, match with the existing left table tuples in the bucket
		    while(rcurs < inputbatch_right.size() ){
		    	righttuple = inputbatch_right.elementAt(rcurs);
		    	//hash the right table tuple using the second hash function
		    	key = Integer.valueOf(String.valueOf(righttuple.dataAt(rightindex)))%(numBuff-2);
		    	currBucket = in_memory[key];
		    	while(lcurs < currBucket.size()){
		    		//match each left table tuple in the partition
		    		lefttuple = currBucket.elementAt(lcurs++);
				    if(lefttuple.checkJoin(righttuple,leftindex,rightindex)){
						Tuple outtuple = lefttuple.joinWith(righttuple);
						outbatch.add(outtuple);
						if(outbatch.isFull()){
							Batch result  = outbatch;
							outbatch = new Batch(batchsize);
						    return result;
						}
					}    
		    	}
		    	
		    	//for current right tuple, finish matching with all left bucket tuples
		    	if(lcurs >= currBucket.size()){
		    		//move on to the next right tuple, reset lcurs to 0
		    		rcurs++;
		    		lcurs = 0;
		    	}
		    	
		    	//finish the current right table page, need to load a new page into buffer
		    	if(rcurs >= inputbatch_right.size()){
					//load a new right table page
					rcurs = 0;
					lcurs = 0;
					try{
						inputbatch_right = (Batch)rightInput.readObject();
						while(inputbatch_right == null || inputbatch_right.isEmpty()) {
							inputbatch_right = (Batch)rightInput.readObject();
						}
						eosr = false;
					
					}catch(IOException io){
						//reach end of right table partition
						eosr = true;
						try{
							rightInput.close();
						}catch(IOException io1){
							System.out.println("HashJoin:error in closing file");
						}
						break;
					} catch (ClassNotFoundException e) {
					    System.out.println("HashJoin:Some error in deserialization ");
					    System.exit(1);
					}
		    	}
		    }
		}
		return outbatch;
    }


    /** Close the operator */
    public boolean close(){
    	//delete all the temp file 
    	for(int i = 0; i < numBuff -1;i++){
    		String right_fname = "HJtempRight-" + String.valueOf(i) + this.hashCode();
    		String left_fname = "HJtempLeft-" + String.valueOf(i) + this.hashCode();
    		File f = new File(right_fname);
    		f.delete();
    		f = new File(left_fname);
    		f.delete();
    	}
	return true;

    }


}

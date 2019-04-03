/** simple buffer manager that distributes the buffers equally among
all the join operators
**/

package qp.optimizer;


public class BufferManager{

    static int numBuffer;
    static int numJoin;
    static int numSortingExceptJoin;

    static int buffPerJoin;
    static int buffForSorting = 0;


    public BufferManager(int numBuffer, int numJoin, boolean isDistinct){
	this.numBuffer = numBuffer;
	this.numJoin = numJoin;
	numSortingExceptJoin += isDistinct ? 1 : 0;
	
    buffPerJoin = numBuffer / (numJoin + numSortingExceptJoin);
    buffForSorting = numBuffer - buffPerJoin * numJoin;
    }

    public static int getBuffersPerJoin(){
    	return buffPerJoin;
    }
    
    public static int getBuffersForSorting() {
        return buffForSorting;
    }

}

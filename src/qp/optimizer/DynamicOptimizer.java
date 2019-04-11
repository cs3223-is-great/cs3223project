/** performs dynamic optimization**/

package qp.optimizer;

import java.io.*;
import java.util.*;
import qp.operators.*;
import qp.utils.*;

public class DynamicOptimizer {
	static SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;          // Number of joins in this query plan
	int numTable;		// Number of Table in this query plan


	Vector<Map<Set<String>, Operator>> spaceAmt;

	public DynamicOptimizer(SQLQuery sqlquery) {
		this.sqlquery = sqlquery;
		//get number of table from sql query
		this.numTable = this.sqlquery.getFromList().size();
		spaceAmt = new Vector<Map<Set<String>, Operator>>(this.numTable);

		for (int i = 0; i < this.numTable; i++) {
			Map<Set<String>, Operator> h_map = new HashMap<Set<String>, Operator>();
			spaceAmt.add(h_map);
		}
	}

	public Operator getOptimizedPlan() {
		Operator root = null;

		//for s = 1
		for (int i = 0; i < numTable; i++) {
			Set<String> namesOfTbl = new HashSet<String>();
			String tblName = (String) this.sqlquery.getFromList().elementAt(i);

			namesOfTbl.add(tblName);

			root = null;
			Scan operator = new Scan(tblName, OpType.SCAN);
			Scan tempOperator = operator;
			String fn = tblName + ".md";
			try {
				ObjectInputStream obj = new ObjectInputStream(new FileInputStream(fn));
				Schema schm = (Schema) obj.readObject();
				operator.setSchema(schm);
				obj.close();
			} catch (Exception e) {
				System.err.println("not reading table (dynamic optimizer) " + fn);
				System.exit(1);
			}
			root = operator;

			Select selectOperator = null; //select operator
			for (int j = 0; j < this.sqlquery.getSelectionList().size(); j++) {

				Condition cn = (Condition) this.sqlquery.getSelectionList().elementAt(j);
				if (cn.getOpType() == Condition.SELECT) {
					if (tblName.equals(cn.getLhs().getTabName())) {
						System.out.println("create select op");
						selectOperator = new Select(operator, cn, OpType.SELECT);
						selectOperator.setSchema(tempOperator.getSchema());
						root = selectOperator;
					}
				}
			}

			spaceAmt.elementAt(0).put(namesOfTbl, root);
		}

		//for s = 2 to N
		for (int i = 1; i < numTable; i++) {
			//get all subset s where |s|=i+1
			Map<Integer, Set<String>> subsets = getSubsets(i + 1);
			//for current i, get the best plan for the corresponding subset s
			for (int j = 0; j < subsets.size(); j++) {
				Set<String> curSubset = subsets.get(j);
				String[] curSubsetArray = curSubset.toArray(new String[0]);
				Operator curRoot = getBestPlan(curSubsetArray);
				if(curRoot==null){
					continue;
				}
				Set<String> namesOfTbl = new HashSet<String>();
				for (int k = 0; k <= i; k++) {
					namesOfTbl.add(curSubsetArray[k]);
				}
				spaceAmt.elementAt(i).put(namesOfTbl, curRoot);
			}
		}
		Set<String> tbl = new HashSet<String>();
		for (int i = 0; i < this.sqlquery.getFromList().size(); i++) {
			tbl.add((String) this.sqlquery.getFromList().elementAt(i));
		}
		return spaceAmt.elementAt(numTable - 1).get(tbl);
	}



	public static Operator makeExecPlan(Operator node) {

		if (node.getOpType() == OpType.JOIN) {
			Operator left = makeExecPlan(((Join) node).getLeft());
			Operator right = makeExecPlan(((Join) node).getRight());
			int joinType = ((Join) node).getJoinType();
			int numbuff = BufferManager.getBuffersPerJoin();
			switch (joinType) {
			case JoinType.NESTEDJOIN:

				NestedJoin nj = new NestedJoin((Join) node);
				nj.setLeft(left);
				nj.setRight(right);
				nj.setNumBuff(numbuff);
				return nj;

			case JoinType.BLOCKNESTED:

				BlockNested bj = new BlockNested((Join) node);
				bj.setLeft(left);
				bj.setRight(right);
				bj.setNumBuff(numbuff);
				return bj;

			case JoinType.INDEXNESTED:

				IndexNested in = new IndexNested((Join) node);
				in.setLeft(left);
				in.setRight(right);
				in.setNumBuff(numbuff);
				return in;

			case JoinType.SORTMERGE:

				SortMergeJoin sm = new SortMergeJoin((Join) node);
				sm.setLeft(left);
				sm.setRight(right);
				sm.setNumBuff(numbuff);
				return sm;

			case JoinType.HASHJOIN:

				HashJoin hj = new HashJoin((Join) node);
				hj.setLeft(left);
				hj.setRight(right);
				hj.setNumBuff(numbuff);
				return hj;
			default:
				return node;
			}
		} else if (node.getOpType() == OpType.SELECT) {
			Operator base = makeExecPlan(((Select) node).getBase());
			((Select) node).setBase(base);
			return node;
		} else if (node.getOpType() == OpType.PROJECT) {
			Operator base = makeExecPlan(((Project) node).getBase());
			((Project) node).setBase(base);
			return node;
		} else {
			return node;
		}
	}


	private Operator getBestPlan(String[] subset) { //to get the best plan for one subset
		int size = subset.length;
		int bestPlanCost = Integer.MAX_VALUE;
		Operator curRoot = null;

		for (int i = 0; i < size; i++) {

			String tableName = subset[i];
			Set<String> oneTable = new HashSet<String>();
			oneTable.add(tableName);

			Set<String> restTables = new HashSet<String>();
			Condition condition = null;
			for (int j = 0; j < size; j++) {
				if (j != i)
					restTables.add(subset[j]);
			}


			Operator preRoot = spaceAmt.elementAt(size - 2).get(restTables);

			if (preRoot == null) continue;



			condition = getJoinCondition(preRoot, oneTable);
			int plancost;
			PlanCost pc = new PlanCost();
			if (condition != null) {
				Join newJoin = new Join(preRoot, spaceAmt.elementAt(0).get(oneTable), condition, OpType.JOIN);
				Schema newSchema = preRoot.getSchema().joinWith(spaceAmt.elementAt(0).get(oneTable).getSchema());
				newJoin.setSchema(newSchema);

				int curCost = 0;
				plancost = Integer.MAX_VALUE;
				int type=0;

				newJoin.setJoinType(JoinType.BLOCKNESTED);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.BLOCKNESTED;
				}

				newJoin.setJoinType(JoinType.NESTEDJOIN);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.NESTEDJOIN;
				}

				newJoin.setJoinType(JoinType.INDEXNESTED);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.INDEXNESTED;
				}

				 newJoin.setJoinType(JoinType.HASHJOIN);
				 pc = new PlanCost();
				 curCost = pc.getCost(newJoin);
				 if (curCost < plancost) {
				 	plancost = curCost;
				 	type = JoinType.HASHJOIN;
				 }

				newJoin.setJoinType(JoinType.SORTMERGE);
				pc = new PlanCost();
				curCost = pc.getCost(newJoin);
				if (curCost < plancost) {
					plancost = curCost;
					type = JoinType.SORTMERGE;
				}

				newJoin.setJoinType(type);

				if (plancost < bestPlanCost) {
					bestPlanCost = plancost;
					curRoot = newJoin;
				}
			}
		}
		return curRoot;
	}

	private Condition getJoinCondition(Operator preRoot, Set<String> oneTable) {
		for (int i = 0; i < this.sqlquery.getJoinList().size(); i++) {
			Condition con = (Condition) this.sqlquery.getJoinList().elementAt(i);
			Attribute leftjoinAttr = con.getLhs();
			Attribute rightjoinAttr = (Attribute) con.getRhs();
			if ((preRoot.getSchema().contains(leftjoinAttr) && (spaceAmt.elementAt(0).get(oneTable).getSchema().contains(rightjoinAttr)))) {
				return (Condition) con.clone();
			} else if (preRoot.getSchema().contains(rightjoinAttr) && spaceAmt.elementAt(0).get(oneTable).getSchema().contains(leftjoinAttr)) {
				Condition newCon = (Condition) con.clone();
				newCon.setRhs((Attribute) leftjoinAttr.clone());
				newCon.setLhs((Attribute) rightjoinAttr.clone());
				return newCon;
			}
		}
		return null;
	}


	private Map<Integer, Set<String>> getSubsets(int size) {
		Map<Integer, Set<String>> map = new HashMap<Integer, Set<String>>();
		int i, j;
		int totalNumOfSubset = (int) Math.pow(2, this.numTable);
		int key = 0;
		for (i = 0; i < totalNumOfSubset; i++) {
			if (size == countNumOfOnes(i)) {//one possible subset
				Set<String> set = new HashSet<String>();
				for (j = 0; j < this.numTable; j++) {//find the corresponding table name and add into the hashset
					if ((i & (1 << j)) != 0)
						set.add((String) sqlquery.getFromList().get(j));
				}
				map.put(key, set);
				key++;
			}
		}
		return map;
	}

	private int countNumOfOnes(int i) {
		int totalNumOfOnes = 0;
		while (i != 0) {
			if ((i & 1) != 0){
				totalNumOfOnes++;
			}
			i >>= 1;
		}
		return totalNumOfOnes;
	}
}

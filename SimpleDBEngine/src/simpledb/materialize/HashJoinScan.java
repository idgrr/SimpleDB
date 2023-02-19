package simpledb.materialize;

import simpledb.query.Predicate;
import simpledb.multibuffer.MultibufferProductScan;
import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Scan;
import simpledb.query.SelectScan;
import simpledb.query.Term;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>hashJoin</i> operator.
 * @author Irfan, Lucas
 */
public class HashJoinScan implements Scan {
    private TempTable[] lhs;
    private TempTable[] rhs;
    private int currBucket;
    private Transaction tx;
    private Predicate pred;
    private SelectScan sc;
    private MultibufferProductScan mb;
    
    /**
     * Constructor for HashJoinScan. Takes in buckets of lhs and rhs. It does productScan and 
     * layers a select scan to return only when the join predicate is satisfied
     * 
     * @param lhsTable lhs table sorted in buckets 
     * @param rhsTable rhs table sorted in buckets 
     * @param lhsFld joinfieldtype of left table
     * @param rhsFld joinfieldtype of right table
     * @param tx
     */
    public HashJoinScan(TempTable[] lhsTable, TempTable[] rhsTable, String lhsFld, String rhsFld, Transaction tx) {
			lhs = lhsTable;
			rhs = rhsTable;
			currBucket = 0;
			this.tx = tx;
			Term term = new Term(new Expression(lhsFld), new Expression(rhsFld), "=");
			this.pred = new Predicate(term);
			while(lhs[currBucket] == null || rhs[currBucket] == null)
				currBucket++;
			UpdateScan lhsScan = lhs[currBucket].open();
			Layout rhsLayout = rhs[currBucket].getLayout();
			mb = new MultibufferProductScan(tx, lhsScan, rhs[currBucket].tableName(), rhsLayout);
			sc = new SelectScan(mb, pred);
}
    /**
     * Goes to the first bucket which is 0
     */
	@Override
	public void beforeFirst() {
        currBucket = 0;
	}
	
	/**
	 * Moves the pointer to the next tuple that in the bucket that satisfies the 
	 * join predicate. If there are no more, in the bucket, will move on to  the next
	 * bucket and repeat. will return false only when no more buckets AND no more tuples that
	 * satisfy
	 */
	@Override
    public boolean next() {
        boolean hasmore = sc.next();
        if (!hasmore) {
        	currBucket++;
            sc.close();
            if(currBucket < lhs.length) {
                UpdateScan lhsScan = lhs[currBucket].open();
                Layout rhsLayout = rhs[currBucket].getLayout();
                MultibufferProductScan mb = new MultibufferProductScan(tx, lhsScan, rhs[currBucket].tableName(), rhsLayout);
                sc = new SelectScan(mb, pred);
                return sc.next();
            } else {
            	return false;
            }
        }
        return hasmore;
    }

	@Override
    public int getInt(String fldname) {
        return sc.getInt(fldname);
    }

	@Override
    public String getString(String fldname){
        return sc.getString(fldname);
    }

	@Override
    public Constant getVal(String fldname) {
        return sc.getVal(fldname);
    }

	@Override
    public boolean hasField(String fldname) {
        return sc.hasField(fldname);
    }

	@Override
    public void close() {
		sc.close();
	}

}

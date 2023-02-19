package simpledb.materialize;

import simpledb.multibuffer.BufferNeeds;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the <i>Hashjoin</i> operator.
 * 
 * @author Irfan, Lucas
 */
public class HashJoinPlan implements Plan {
	private Transaction tx;
	private int blocksAccessed;
	private String rhsFld;
	private String lhsFld;
	private Schema sch;
	private Plan lhs;
	private Plan rhs;

	/**
	 * Constructor for HashJoinPlan. Checks that P1 is smaller than p2 to enure
	 * efficiency
	 * 
	 * @param tx
	 * @param p1
	 * @param p2
	 * @param fldname1
	 * @param fldname2
	 */
	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		sch = new Schema();
		if (p1.blocksAccessed() < p2.blocksAccessed()) {
			lhs = p1;
			rhs = p2;
			lhsFld = fldname1;
			rhsFld = fldname2;
			blocksAccessed = p1.blocksAccessed();
		} else {
			lhs = p2;
			rhs = p1;
			rhsFld = fldname1;
			lhsFld = fldname2;
			blocksAccessed = p2.blocksAccessed();
		}
		sch.addAll(rhs.schema());
		sch.addAll(lhs.schema());
		this.tx = tx;
	}

	/**
	 * Splits both the lhs and rhs into buckets.
	 */
	@Override
	public Scan open() {
		int buckets = BufferNeeds.bestRoot(tx.availableBuffs(), blocksAccessed);
		TempTable[] tempLHS = splitIntoBuckets(buckets, lhs, lhsFld);
		TempTable[] tempRHS = splitIntoBuckets(buckets, rhs, rhsFld);
		return new HashJoinScan(tempLHS, tempRHS, lhsFld, rhsFld, tx);
	}

	/**
	 * Splits each table into buckets. Number of buckets determined by the buffers
	 * available
	 * 
	 * @param buckets
	 * @param p
	 * @param fld
	 * @return
	 */
	private TempTable[] splitIntoBuckets(int buckets, Plan p, String fld) {
		TempTable[] temp = new TempTable[buckets];
		Schema schema = p.schema();
		Scan src = p.open();
		src.beforeFirst();
		while (src.next()) {
			int val;
			if (sch.type(fld) == 4) { // index is a type 4
				val = src.getInt(fld);
			} else {
				val = src.getString(fld).hashCode();
			}
			int i = val % buckets;
			TempTable t = temp[i];
			if (t == null)
				t = new TempTable(tx, schema);
			UpdateScan sc = t.open();
			copy(src, sc, schema);
			temp[i] = t;
			sc.close();
		}
		return temp;

	}

	private void copy(Scan src, UpdateScan dest, Schema schema) {
		dest.insert();
		for (String fldname : schema.fields()) {
			dest.setVal(fldname, src.getVal(fldname));
		}
	}
	
	/**
	 * Returns number of block accessed. Roughly 2 passes. 1
	 * to sort into buckets and another to probe through
	 */
	@Override
	public int blocksAccessed() {
		return 2*(lhs.blocksAccessed() + rhs.blocksAccessed());
	}
	
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
	@Override
	public int recordsOutput() {
		int maxvals = Math.max(lhs.distinctValues(lhsFld), rhs.distinctValues(rhsFld));
		return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
	}
	
   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
	@Override
	public int distinctValues(String fldname) {
		if (lhs.schema().hasField(fldname))
			return lhs.distinctValues(fldname);
		else
			return rhs.distinctValues(fldname);
	}

	@Override
	public Schema schema() {
		return sch;
	}

}

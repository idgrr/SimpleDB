package simpledb.materialize;

import java.util.Arrays;
import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.materialize.NestedLoopScan;

/**
 * The Plan class for the <i>nestedLoop</i> operator.
 * 
 * @author
 */
public class NestedLoopPlan implements Plan {

	private Plan p1, p2;
	private String fldname1, fldname2;
	private int p1BlocksAccessed = 0;
	private int p1RecordsAccessed = 0;
	private int p2BlocksAccessed = 0;
	private int p2RecordsAccessed = 0;
	private Schema sch = new Schema();
	private Transaction tx;

	public NestedLoopPlan(Transaction tx, Plan p1, Plan p2, String fld1, String fld2) {

		p1BlocksAccessed = p1.blocksAccessed();
		p2BlocksAccessed = p2.blocksAccessed();
		if (p1BlocksAccessed < p2BlocksAccessed) {
			this.fldname1 = fld1;
			this.p1 = p1;
			this.fldname2 = fld2;
			this.p2 = p2;
			sch.addAll(p1.schema());
			sch.addAll(p2.schema());
		} else {
			this.fldname1 = fld2;
			this.p1 = p2;
			this.fldname2 = fld1;
			this.p2 = p1;
			p1BlocksAccessed = p2.blocksAccessed();
			p2BlocksAccessed = p1.blocksAccessed();
			sch.addAll(p2.schema());
			sch.addAll(p1.schema());
		}

		p1RecordsAccessed = p1.recordsOutput();
		p2RecordsAccessed = p2.recordsOutput();

		this.tx = tx;
	}

	@Override
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		return new NestedLoopScan(s1, s2, fldname1, fldname2);
	}

	/**
	 * As this is a SimpleNested join, it reads all the blocks of p1 once and for
	 * each record, it reads p2 once
	 */
	@Override
	public int blocksAccessed() {
		int blocks = p1BlocksAccessed + p1RecordsAccessed * p2BlocksAccessed;
		return blocks > 0 ? blocks : 1;
	}

	/**
	 * Worst case where each record in p1 is joined with all records in p2
	 */
	@Override
	public int recordsOutput() {
		int records = p1RecordsAccessed * p2BlocksAccessed;
		return records > 0 ? records : 1;
	}

	/**
	 * Estimate the distinct number of field values in the join. Since the join does
	 * not increase or decrease field values, the estimate is the same as in the
	 * appropriate underlying query.
	 * 
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	@Override
	public int distinctValues(String fldname) {
		int maxvals = Math.max(p1.distinctValues(fldname), p2.distinctValues(fldname));
		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	}

	@Override
	public Schema schema() {
		return sch;
	}

}

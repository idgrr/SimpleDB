package simpledb.materialize;

import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class DistinctPlan implements Plan {
	
	
   private SortPlan p;
   private List<String> distinctfieldlist;
   private Schema sch;

	public DistinctPlan(Transaction tx, Plan p, List<String> distinctfieldlist) {
		      this.p = new SortPlan(tx, p, distinctfieldlist);
		      this.distinctfieldlist = distinctfieldlist;
		      this.sch = p.schema();
	}


	@Override
	public Scan open() {
	      Scan s = p.open();
	      return new DistinctScan(s, distinctfieldlist);
	}

	@Override
	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		return p.recordsOutput();
	}

	@Override
	public int distinctValues(String fldname) {
	      if (p.schema().hasField(fldname))
	          return p.distinctValues(fldname);
		return 0;
	}

	@Override
	public Schema schema() {
		return sch;
	}

}

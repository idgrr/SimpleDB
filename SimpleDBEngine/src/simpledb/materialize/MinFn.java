package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class MinFn implements AggregationFn {
   private String fldname;
   private Constant val;
   /**
    * Create a max aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public MinFn(String fldname) {
      this.fldname = fldname;
   }
	   
	public void processFirst(Scan s) {
		val = s.getVal(fldname);

	}

	@Override
	public void processNext(Scan s) {
      Constant newval = s.getVal(fldname);
      if (newval.compareTo(val) < 0)
         val = newval;
   }

	@Override
	public String fieldName() {
		return "minof" + fldname;
	}

	@Override
	public Constant value() {
		return val;
	}

}

package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class SumFn implements AggregationFn {
	private String fldname;
	private int sum;
   
   /**
    * Create a sum aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public SumFn(String fldname) {
      this.fldname = fldname;
   }
	   
	public void processFirst(Scan s) {
		sum = s.getVal(fldname).asInt();
	}

	@Override
	public void processNext(Scan s) {
		sum += s.getVal(fldname).asInt();

	}

	@Override
	public String fieldName() {
	      return "sumof" + fldname;

	}

	@Override
	public Constant value() {
		return new Constant(sum);
	}

}

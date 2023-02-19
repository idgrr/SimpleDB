package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class AvgFn implements AggregationFn {
	private String fldname;
	private int sum;
	private int numEle;
   
   /**
    * Create a count aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
   }
	   
	public void processFirst(Scan s) {
		sum = s.getVal(fldname).asInt();
		numEle = 1;
	}

	@Override
	public void processNext(Scan s) {
		sum += s.getVal(fldname).asInt();
		numEle += 1;

	}

	@Override
	public String fieldName() {
	      return "sumof" + fldname;

	}

	@Override
	public Constant value() {
		return new Constant(sum/numEle);
	}

}

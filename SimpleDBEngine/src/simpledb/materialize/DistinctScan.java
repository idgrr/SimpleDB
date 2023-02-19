package simpledb.materialize;

import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class DistinctScan implements Scan {
	private List<String> distinctfieldlist;
	private Scan s;
	private Constant[] current;
	private Boolean first;

	public DistinctScan(Scan s, List<String> distinctfieldlist) {
		this.distinctfieldlist = distinctfieldlist;
		this.s = s;
		beforeFirst();
		
	}

	@Override
	public void beforeFirst() {
	    s.beforeFirst();
	    current = new Constant[distinctfieldlist.size()];
	    first = true;
	}

	@Override
	public boolean next() {
		Boolean hasmore = s.next();

		while(hasmore && !shouldPrint(s,current) && !first) {
			hasmore = s.next();
		}
		if(!hasmore) {
			return false;
		}
		for(int i = 0; i < distinctfieldlist.size(); i++) { //populate new current to compare
				current[i] = s.getVal(distinctfieldlist.get(i));
			}
		first = false;
		return true;

		
	}
	
	private boolean shouldPrint(Scan s,  Constant[] current) {
		for (int i = 0; i < distinctfieldlist.size(); i++) {
			String fldname = distinctfieldlist.get(i);
			Constant sval = s.getVal(fldname);
			Constant cval = current[i];
			if(!sval.equals(cval)) {
				return true;
				}
			}
		return false;
	}

	@Override
	public int getInt(String fldname) {
		return s.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		return s.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (s.hasField(fldname)) {
		return s.getVal(fldname);
		}
		return null;
	}

	@Override
	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}

	@Override
	public void close() {
		s.close();

	}

}

package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class NestedLoopScan implements Scan {
	private Scan s1;
	private Scan s2;
	private String fldname1, fldname2;
	private Constant joinval = null;
	private Boolean lhsHasMore = false;
	private Boolean rhsHasMore = false;
	private Boolean shouldScan = true;

	public NestedLoopScan(Scan s1, Scan s2, String fldname1, String fldname2) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		beforeFirst();
	}

	/**
	 * position the scan before first record
	 */
	@Override
	public void beforeFirst() {
		shouldScan = canScan();
		s1.beforeFirst();
		s2.beforeFirst();
		this.lhsHasMore = s1.next();
		this.rhsHasMore = false;
	}

	@Override
	public boolean next() {

		if (!shouldScan) {
			return false;
		}

		rhsHasMore = s2.next();
		if (lhsHasMore && rhsHasMore) {
			Constant v1 = s1.getVal(fldname1);
			Constant v2 = s2.getVal(fldname2);
			if (v1.compareTo(v2) == 0)
				return true;
			else {
				return this.next();
			}
		}
		lhsHasMore = s1.next();
		if (lhsHasMore) {
			s2.beforeFirst();
			return this.next();
		}
		return false;
	}

	/**
	 * Checks if the scan has a next before returning any value/calling the next
	 * scan
	 * 
	 * @return true if there are next records on both s1 and s2
	 */
	private boolean canScan() {
		s1.beforeFirst();
		s2.beforeFirst();
		return s1.next() && s2.next();
	}

	@Override
	public int getInt(String fldname) {
		if (s1.hasField(fldname))
			return s1.getInt(fldname);
		else
			return s2.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (s1.hasField(fldname))
			return s1.getString(fldname);
		else
			return s2.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (s1.hasField(fldname))
			return s1.getVal(fldname);
		else
			return s2.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}

	@Override
	public void close() {
		s1.close();
		s2.close();
	}

}

package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.materialize.NestedLoopPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
	private TablePlan myplan;
	private Predicate mypred;
	private Schema myschema;
	private Map<String, IndexInfo> indexes;
	private Transaction tx;
	private String joinType = "";

	/**
	 * Creates a new table planner. The specified predicate applies to the entire
	 * query. The table planner is responsible for determining which portion of the
	 * predicate is useful to the table, and when indexes are useful.
	 * 
	 * @param tblname the name of the table
	 * @param mypred  the query predicate
	 * @param tx      the calling transaction
	 */
	public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
		this.mypred = mypred;
		this.tx = tx;
		myplan = new TablePlan(tx, tblname, mdm);
		myschema = myplan.schema();
		indexes = mdm.getIndexInfo(tblname, tx);
	}

	/**
	 * Constructs a select plan for the table. The plan will use an index select, if
	 * possible.
	 * 
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelect();
		if (p == null)
			p = myplan;
		return addSelectPred(p);
	}

	/**
	 * Constructs a join plan of the specified plan and the table. The plan will use
	 * an indexjoin, if possible. (Which means that if an indexselect is also
	 * possible, the indexjoin operator takes precedence.) The method returns null
	 * if no join is possible.
	 * @param current the specified plan
	 * @return a join plan of the plan and this table
	 */
	public Plan makeJoinPlan(Plan current) {
		Schema currsch = current.schema();
		String currentAtt = "";
		String myplanAtt = "";
		String opr = "=";
		Boolean flag = true;
		
		// determine which of attribute belongs to which table
		for (Term term : mypred.terms()) {
			if (!term.lhs().isFieldName() || !term.rhs().isFieldName()) {
				opr = term.opr();
				if (!opr.equals("=")) {
					flag = false;
				}
				continue;
			}
			currentAtt = term.lhs().toString();
			myplanAtt = term.rhs().toString();
			if (checkTerms(current, myplan, currentAtt, myplanAtt)) {
				if (!current.schema().fields().contains(currentAtt)) {
					String temp = currentAtt;
					currentAtt = myplanAtt;
					myplanAtt = temp;
				}
				break;
			}
		}
		
		// checks through all the predicates. This flag is used to check 
		// whether hash join can be considered. (hash join equalityPredicates only)
		for (Term term : mypred.terms()) {
			if (!opr.equals("=")) {
				flag = false;
				break;
			}

		}

		// returns null if none of the predicates in the where clause can join the two tables
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null || currentAtt == "" || myplanAtt == "")
			return null;
		
		// plans
		Plan indexJoin = makeIndexJoin(current, currsch); // Index-based join
		Plan sortMergeJoin = makeSortMergeJoin(current, currsch, currentAtt, myplanAtt);
		Plan crossJoin = makeProductJoin(current, currsch); // cross product
		Plan nestedTupleJoin = makeNestedTupleJoin(current, currsch, currentAtt, myplanAtt);
		Plan hashJoin = makeHashJoin(current, currsch, currentAtt, myplanAtt);


		Plan bestPlan = crossJoin;
		joinType = "cross Join";
		if (nestedTupleJoin.blocksAccessed() < bestPlan.blocksAccessed()) {
			bestPlan = nestedTupleJoin;
			joinType = "Nested Loop Join";
		}

		if (indexJoin != null && indexJoin.blocksAccessed() < bestPlan.blocksAccessed()) {
			bestPlan = indexJoin;
			joinType = "Index Join";
		}

		if (sortMergeJoin.blocksAccessed() < bestPlan.blocksAccessed()) {
			bestPlan = sortMergeJoin;
			joinType = "Sort Merge Join";
		}

		if (crossJoin.blocksAccessed() < bestPlan.blocksAccessed()) {
			bestPlan = crossJoin;
			joinType = "Cartesian Join";
		}
		if (flag && hashJoin.blocksAccessed() < bestPlan.blocksAccessed()) {
			bestPlan = hashJoin;
			joinType = "Hash Join";
		}
		return bestPlan;
	}
	
	/**
	 * Similar to makePlanTest but jointype can be determined externally.
	 * This is used for testing
	 * @param current
	 * @param type
	 * @return
	 */
	public Plan makeJoinPlanTest(Plan current, String type) {
		Schema currsch = current.schema();
		String currentAtt = "";
		String myplanAtt = "";
		String opr = "";
		Boolean flag = true;
		// determine which of attribute belongs to which table
		for (Term term : mypred.terms()) {
			if (!term.lhs().isFieldName() || !term.rhs().isFieldName()) {
				opr = term.opr();
				if (!opr.equals("=")) {
					flag = false;
				}
				continue;
			}
			currentAtt = term.lhs().toString();
			myplanAtt = term.rhs().toString();
			if (checkTerms(current, myplan, currentAtt, myplanAtt)) {
				if (!current.schema().fields().contains(currentAtt)) {
					String temp = currentAtt;
					currentAtt = myplanAtt;
					myplanAtt = temp;
				}
				break;
			}
		}

		for (Term term : mypred.terms()) {
			if (!opr.equals("=")) {
				flag = false;
				break;
			}

		}
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null || currentAtt == "" || myplanAtt == "")
			return null;

		switch (type) {
		case "nested":
			return makeNestedTupleJoin(current, currsch, currentAtt, myplanAtt);
		case "index":
			return makeIndexJoin(current, currsch);
		case "sort":
			return makeSortMergeJoin(current, currsch, currentAtt, myplanAtt);
		case "hash":
			return makeHashJoin(current, currsch, currentAtt, myplanAtt);
		default:
			return makeProductJoin(current, currsch);
		}
	}
	
	/**
	 * Checks if both terms are fields of either of the Plans p1 or p2
	 * @return true if it does contain
	 */
	private Boolean checkTerms(Plan p1, Plan p2, String t1, String t2) {
		return (p1.schema().fields().contains(t1) && p2.schema().fields().contains(t2))
				|| (p1.schema().fields().contains(t2) && p2.schema().fields().contains(t1));
	}
	
	
	// Join plans
	
	
	private Plan makeNestedTupleJoin(Plan current, Schema currsch, String lhs, String rhs) {
		Plan p = new NestedLoopPlan(tx, current, myplan, lhs, rhs);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}

	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinPred(p, currsch);
	}

	private Plan makeHashJoin(Plan current, Schema currsch, String lhs, String rhs) {
		Plan p = new HashJoinPlan(tx, current, myplan, lhs, rhs);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}

	public Plan makeProductPlan(Plan current) {
		Plan p = addSelectPred(myplan);
		return new ProductPlan(current, p);
	}

	private Plan makeIndexJoin(Plan current, Schema currsch) {
		for (String fldname : indexes.keySet()) {
			String outerfield = mypred.equatesWithField(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}
	
	private Plan makeSortMergeJoin(Plan current, Schema currsch, String lhs, String rhs) {
		Plan p = new MergeJoinPlan(tx, current, myplan, lhs, rhs);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}
	
	
	private Plan makeIndexSelect() {
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				System.out.println("index on " + fldname + " used");
				return new IndexSelectPlan(myplan, ii, val);
			}
		}
		return null;
	}

	private Plan addSelectPred(Plan p) {
		Predicate selectpred = mypred.selectSubPred(myschema);
		if (selectpred != null)
			return new SelectPlan(p, selectpred);
		else
			return p;
	}

	private Plan addJoinPred(Plan p, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		if (joinpred != null)
			return new SelectPlan(p, joinpred);
		else
			return p;
	}

	// for Display

	public String tableName() {
		return myplan.tableName();
	}

	public String joinType() {
		return joinType;
	}
	
	@Override
	public String toString() {
		return myplan.toString();

	}
}

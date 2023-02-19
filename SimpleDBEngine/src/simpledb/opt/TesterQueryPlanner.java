package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.materialize.AggregationFn;
import simpledb.materialize.DistinctPlan;
import simpledb.materialize.GroupByPlan;
import simpledb.materialize.SortPlan;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * 
 * @author Irfan, Lucas
 */
public class TesterQueryPlanner implements QueryPlanner {
	private List<TablePlanner> tableplanners = new ArrayList<>();
	private MetadataMgr mdm;
	private String currentTableName = "";

	public TesterQueryPlanner(MetadataMgr mdm) {
		this.mdm = mdm;
	}

	/**
	 * Creates an optimized left-deep query plan using the following heuristics. H1.
	 * Choose the smallest table (considering selection predicates) to be first in
	 * the join order. H2. Add the table to the join order which results in the
	 * smallest output.
	 */
	public Plan createPlan(QueryData data, Transaction tx) {

		// Step 1: Create a TablePlanner object for each mentioned table
		String tableUsed[] = { "student", "course", "dept", "section" };
		for (String tblname : tableUsed) {
			TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
			tableplanners.add(tp);
		}
		TablePlanner studentTP = new TablePlanner("student", data.pred(), tx, mdm);
		TablePlanner courseTP = new TablePlanner("course", data.pred(), tx, mdm);
		TablePlanner deptTP = new TablePlanner("dept", data.pred(), tx, mdm);
		TablePlanner sectionTP = new TablePlanner("section", data.pred(), tx, mdm);

		// Step 2: Choose the lowest-size plan to begin the join order
		Plan currentplan = getStartingPlan(deptTP);

		// Step 3: Manually Add
		// "sort", "index", "index" , others= product
		currentplan = getLowestJoinPlan(currentplan, "sort", courseTP);
		currentplan = getLowestJoinPlan(currentplan, "sort", studentTP);
		currentplan = getLowestJoinPlan(currentplan, "nested", sectionTP);

		// Step 4: Add a selection plan for the predicate
		currentplan = new SelectPlan(currentplan, data.pred());

//      // Step 5.  Project on the field names and return

		if (data.distinctfieldlist().size() > 0) {
			currentplan = new DistinctPlan(tx, currentplan, data.distinctfieldlist());
		}

		// Step 6. Add a group by plan?
		if (data.aggfn().size() > 0) {
			currentplan = new GroupByPlan(tx, currentplan, data.groupfields(), data.aggfn());
		}

		// Step 7. Add an order by plan?
		if (data.order().size() > 0) {
			currentplan = new SortPlan(tx, currentplan, data.fields(), data.order());
		}

		List<String> ls = new ArrayList<String>();
		for (AggregationFn fn : data.aggfn())
			ls.add(fn.fieldName());
		for (String fld : data.fields())
			ls.add(fld);
		for (String fld : data.distinctfieldlist())
			ls.add(fld);

		currentplan = new ProjectPlan(currentplan, ls);

		return currentplan;

	}

	private Plan getStartingPlan(TablePlanner p) {
		displaySelectPlanConsideration();
		currentTableName = "";
		TablePlanner tp = p;
		Plan plan = tp.makeSelectPlan();
		displayPlanConsideration(plan, tp);

		System.out.print("Table used as starting : " + tp.tableName());
		currentTableName = tp.tableName();
		String str = "---";
		System.out.print("\n" + str.repeat(10));
		tableplanners.remove(tp);
		return plan;
	}

	private void displaySelectPlanConsideration() {
		System.out.println("Finding the table with the smallest select output : \n");
	}

	private void displayPlanConsideration(Plan plan, TablePlanner tp) {
		String tableInfo = tp.toString();
		int recordsOutput = plan.recordsOutput();

		System.out.println(tableInfo + "\nRecords after AccessPlan = " + recordsOutput + "\n");
	}

	private Plan getLowestJoinPlan(Plan current, String type, TablePlanner p) {
		TablePlanner tp = p;
		Plan plan = tp.makeJoinPlanTest(current, type);
		displayJoinConsideration(plan, tp, current);
		if (plan != null) {
			tableplanners.remove(tp);
			displayJoinDecision(plan, tp, type);
			currentTableName += "X" + tp.tableName();
		}
		return plan;
	}

	private void displayJoinDecision(Plan bestplan, TablePlanner besttp, String type) {
		System.out.println("\nDecided to go with " + type + " with " + currentTableName + " and " + besttp.tableName());
		String str = "-";
		String repeated = str.repeat(30);
		System.out.println(repeated + "\n" + repeated);

	}

	private void displayJoinConsideration(Plan plan, TablePlanner tp, Plan current) {
		System.out.println("\nConsidering " + tp.joinType() + " with " + currentTableName + " and " + tp.tableName());
		if (plan == null) {
			System.out.print("But cannot directly join\n");
		} else {
			System.out.print("Blocks Accessed using this merge : " + plan.blocksAccessed() + "\n");
		}
	}

	private Plan getLowestProductPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeProductPlan(current);
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
	}

	public void setPlanner(Planner p) {
		// for use in planning views, which
		// for simplicity this code doesn't do.
	}
}

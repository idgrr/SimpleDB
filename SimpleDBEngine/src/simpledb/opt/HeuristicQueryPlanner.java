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
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
	private Collection<TablePlanner> tableplanners = new ArrayList<>();
	private MetadataMgr mdm;
	private String currentTableName = "";

	public HeuristicQueryPlanner(MetadataMgr mdm) {
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
		for (String tblname : data.tables()) {
			TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
			tableplanners.add(tp);
		}

		// Step 2: Choose the lowest-size plan to begin the join order
		Plan currentplan = getLowestSelectPlan();

		// Step 3: Repeatedly add a plan to the join order
		while (!tableplanners.isEmpty()) {
			Plan p = getLowestJoinPlan(currentplan);
			if (p != null)
				currentplan = p;
			else // no applicable join
				currentplan = getLowestProductPlan(currentplan);
		}

		// Step 4: Add a selection plan for the predicate
		currentplan = new SelectPlan(currentplan, data.pred());

		// Step 5. Project on the field names and return

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

		// Ensure that all relevant fields are projected
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

	/**
	 * Finds which table will have the lowest Select Plan Used Primary to get the
	 * first table to start join on
	 * @return selectplan on the smallest table
	 */
	private Plan getLowestSelectPlan() {
		currentTableName = "";
		TablePlanner besttp = null;
		Plan bestplan = null;
		displaySelectPlanConsideration();
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeSelectPlan();
			displayPlanConsideration(plan, tp);
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;

			}
		}
		System.out.print("Initial Table found based on smallest Select : " + besttp.tableName());
		currentTableName = besttp.tableName();
		String str = "---";
		System.out.print("\n" + str.repeat(10) + "\n");
		tableplanners.remove(besttp);
		return bestplan;
	}
	
	/**
	 * Iterate through all the remaining tables and see which join and table is the next best join
	 * @param current table that is being considered
	 * @return best next table join
	 */
	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeJoinPlan(current);
			displayJoinConsideration(plan, tp, current);
			if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
				besttp = tp;
				bestplan = plan;
			}
		}
		if (bestplan != null) {
			tableplanners.remove(besttp);
			displayJoinDecision(bestplan, besttp);
			currentTableName += "X" + besttp.tableName();
		}
		return bestplan;
	}
	
	/**
	 * Iterate through all the remaining tables and see which table is best to crossjoin with.
	 * Usually done after considering all available joins
	 * @param current
	 * @return
	 */
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

	//DisplayFuctions
	
	private void displaySelectPlanConsideration() {
		System.out.println("Finding the table with the smallest select output : \n");
	}

	private void displayPlanConsideration(Plan plan, TablePlanner tp) {
		String tableInfo = tp.toString();
		int recordsOutput = plan.recordsOutput();
		System.out.println(tableInfo + "\nRecords after AccessPlan = " + recordsOutput + "\n");
	}
	
	private void displayJoinConsideration(Plan plan, TablePlanner tp, Plan current) {
		System.out.println("\nConsidering " + tp.joinType() + " with " + currentTableName + " and " + tp.tableName());
		if (plan == null) {
			System.out.print("But cannot directly join\n");
		} else {
			System.out.print("Blocks Accessed using this merge : " + plan.blocksAccessed() + "\n");
		}
	}
	
	private void displayJoinDecision(Plan bestplan, TablePlanner besttp) {
		System.out.println("\nDecided to go with " + besttp.joinType() + "with " + currentTableName + " and "
				+ besttp.tableName());
		String str = "-";
		String repeated = str.repeat(30);
		System.out.println(repeated + "\n" + repeated);

	}
	
	
	
	public void setPlanner(Planner p) {
		// for use in planning views, which
		// for simplicity this code doesn't do.
	}
}

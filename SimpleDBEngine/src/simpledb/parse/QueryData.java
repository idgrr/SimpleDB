package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> order;
   private List<AggregationFn> aggfn;
   private List<String> groupfields;
   private List<String> distinctFieldList;
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.order = new ArrayList<String>();
   }
   
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> order) {
	      this.fields = fields;
	      this.tables = tables;
	      this.pred = pred;
	      this.order = order;
	   }
   
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> order, List<AggregationFn> aggfn, List<String> groupfields) {
	      this.fields = fields;
	      this.tables = tables;
	      this.pred = pred;
	      this.order = order;
	      this.aggfn = aggfn;
	      this.groupfields = groupfields;
	   }
   
   
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> order,
		List<AggregationFn> aggfn, List<String> groupfields, List<String> distinctFieldList) {
	      this.fields = fields;
	      this.tables = tables;
	      this.pred = pred;
	      this.order = order;
	      this.aggfn = aggfn;
	      this.groupfields = groupfields;
	      this.distinctFieldList = distinctFieldList;
   		}
   
   public List<String> distinctfieldlist() {
		return distinctFieldList;
	}

   public List<AggregationFn> aggfn(){
	   return aggfn;
   }
   
   public List<String> groupfields(){
	   return groupfields;
   }
   
   public List<String> order(){
	   return order;
   }
	   
   
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }


}

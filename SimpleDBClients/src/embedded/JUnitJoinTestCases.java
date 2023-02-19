package embedded;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;


/**
 * Junit Tester Class.
 * Run by user to ensure that any new changes made does not affect other functions
 * These test cases are not exhaustive and merely chosen based on past errors encountered
 * @author Irfan, Lucas
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class JUnitJoinTestCases {

	private static List<String>  output = new ArrayList<String>();
	private static List<String>  expected = new ArrayList<String>();
	private Transaction tx;
	private Planner planner;
	private String result = "";
	
	/**
	 * Setup student DB. Create break points  to check which planner DB is on
	 * Should usually be on HeuresticQueryPLanner
	 * @throws Exception if tehre are errors connecting to the current database
	 */
	@BeforeEach
	void setUp() throws Exception {
	    Driver d = new EmbeddedDriver();
	    String url = "jdbc:simpledb:studentdb";
        SimpleDB db = new SimpleDB("studentdb");
         tx = db.newTx();
        planner = db.planner();
	}
	
	@Test
	void t1selectionPlan() throws Exception{
         String qry = "select sname from student where sid = 1";
         result = "";
         Plan p = planner.createQueryPlan(qry, tx);
         Scan s1 = p.open();
         while (s1.next()) {
           result += s1.getString("sname") + " ";
         }
         output.add(result);
         s1.close();
	}
	
	@Test
	void t2multipleSelectionPlan() throws Exception{
        String qry = "select sname from student where sid = 2 and majorid = 20 ";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getString("sname") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	@Test
	void t3joinWithSelectPlanDistinct() throws Exception{
        String qry = "select distinct sname from student, enroll where sid = studentid  and gradyear = 2023 and majorid = 27";
         String result = "";
         Plan p = planner.createQueryPlan(qry, tx);
         Scan s1 = p.open();
         while (s1.next()) {
           result += s1.getString("sname") + " ";
         }
         output.add(result);
         s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t4groupBy() throws Exception{
		String qry = "select majorid, count(sid) from student group by majorid";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getInt("majorid") + " " + s1.getInt("countofsid") + " ";
        };
        output.add(result);
        s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t5joinWithSelectPlanDuplicates() throws Exception{
		String qry = "select sname from student,course where deptid = majorid and gradyear = 2020 and sid = 2";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getString("sname") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t6maxGroupBy() throws Exception{
        String qry = "select majorid, max(sid) from student group by majorid";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getInt("majorid")  + " " + s1.getInt("maxofsid") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t7sumGroupBy() throws Exception{
        String qry = "select sum(sid) from student group by majorid";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getInt("sumofsid") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t8maxGroupBy2() throws Exception{
        String qry = "select max(majorid) from student group by sname";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getInt("maxofmajorid") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	@Test // REMEMBER TO COPY TEST
	void t9distinct() throws Exception{
        String qry = "select distinct sid from student";
        result = "";
        Plan p = planner.createQueryPlan(qry, tx);
        Scan s1 = p.open();
        while (s1.next()) {
          result += s1.getInt("sid") + " ";
        }
        output.add(result);
        s1.close();
	}
	
	
	@AfterEach
	void cleanup() {
        tx.commit();
        tx.recover();
	}
	
	
	@AfterAll
	static void assertOutput() {
		inputExpected("joe "); //SimpleSelect
		inputExpected("amy "); // multipleSelect
		inputExpected("Althea Inez "); // btest
		inputExpected("10 5 11 4 13 1 14 2 15 1 16 2 17 1 18 1 19 3 20 7 21 4 22 2 23 1 24 2 25 1 26 2 27 6 28 3 30 2 623 1 ");
		inputExpected("amy amy ");
		inputExpected("10 41 11 32 13 16 14 49 15 29 16 47 17 50 18 37 19 43 20 39 21 44 22 30 23 18 24 34 25 46 26 48 27 42 28 36 30 7 623 51 ");
		inputExpected("92 81 16 80 29 58 50 37 96 101 117 57 18 44 46 93 153 96 12 51 ");
		inputExpected("25 27 20 26 16 16 15 28 27 14 17 23 21 21 18 10 28 11 21 27 24 27 10 19 19 27 28 13 20 11 22 14 10 24 11 21 11 20 27 22 19 26 20 30 30 10 20 623 10 20 20 ");
		inputExpected("1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 ");
		
		check();
	}
	
	static void inputExpected(String expect) {
		expected.add(expect);
	}
	
	
	static void check() {
		int passed = 0;
		for(int i = 0; i < output.size() ; i++) {
			passed++;
			String checkTest = output.get(i);
			String expectedTest = expected.get(i);
			if (!checkTest.equals(expectedTest)) {
				passed--;
				System.out.println("test " + (i + 1 ) + " " + checkTest.equals(expectedTest));
				System.out.println("Expected :{" + expectedTest + "} \nResult  :{" + checkTest +"}");
			}
			assertEquals(checkTest, expectedTest);
		}
		System.out.print(passed + "/" + expected.size() +" tests passed");
		
	}
	

}

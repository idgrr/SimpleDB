package embedded;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Test class to test and print singular queries.
 * 
 * @author Irfan, Lucas
 *
 */
public class TestCase {

	public static void main(String[] args) {
		Driver d = new EmbeddedDriver();
		String url = "jdbc:simpledb:studentdb";
		try (Connection conn = d.connect(url, null); Statement stmt = conn.createStatement()) {

			SimpleDB db = new SimpleDB("studentdb");
			Transaction tx = db.newTx();
			Planner planner = db.planner();
			
			// more test cases can be found in report or JunitTestCases
			String qry = "select distinct sname from student, enroll where sid = studentid  and gradyear = 2023 and majorid = 27";

			Plan p = planner.createQueryPlan(qry, tx);
			Scan s1 = p.open();

			while (s1.next()) {
				System.out.println(s1.getString("sname"));
			}
			s1.close();
			tx.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}

package embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateStudentDB {
   public static void main(String[] args) {
      Driver d = new EmbeddedDriver();
      String url = "jdbc:simpledb:studentdb";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");
         
         s = "create index idx_majorid on student(MajorId) using btree";
         stmt.executeUpdate(s);
         System.out.println("MajorID Index created.");
         
         s = "create index sid on student(sid) using hash";
         stmt.executeUpdate(s);
         System.out.println("sid Index created.");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = 
        	 {
        	   "(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
               "  (9,'Tamara',11,2022)",
               "  (10,'Susan',24,2022)",
               "  (11,'Barclay',16,2022)",
               "  (12,'Desiree',10,2022)",
               "  (13,'Yvonne',19,2021)",
               "  (14,'Heather',27,2022)",
               "  (15,'Christopher',21,2022)",
               "  (16,'Randall',13,2022)",
               "  (17,'Amal',20,2021)",
               "  (18,'Chase',23,2021)",
               "  (19,'Shad',11,2023)",
               "  (20,'Fitzgerald',21,2022)",
               "  (21,'Ulla',11,2021)",
               "  (22,'Marvin',27,2022)",
               "  (23,'Althea',27,2023)",
               "  (24,'Yardley',27,2022)",
               "  (25,'Xander',20,2021)",
               "  (26,'Drew',28,2021)",
               "  (27,'Yoshio',22,2022)",
               "  (28,'Caryn',27,2021)",
               "  (29,'Brock',15,2022)",
               "  (30,'Shannon',22,2023)",
               "  (31,'Stephen',14,2023)",
               "  (32,'Erich',11,2022)",
               "  (34,'Preston',28,2022)",
               "  (34,'Hop',24,2023)",
               "  (35,'Steven',10,2021)",
               "  (36,'Bruce',28,2023)",
               "  (37,'Desirae',18,2022)",
               "  (38,'Daquan',21,2021)",
               "  (39,'Selma',20,2023)",
               "  (40,'Jeanette',19,2021)",
               "  (41,'James',10,2022)",
               "  (42,'Inez',27,2023)",
               "  (43,'Joel',19,2023)",
               "  (44,'Tana',21,2023)",
               "  (45,'Aurora',26,2022)",
               "  (46,'Alan',25,2023)",
               "  (47,'Bevis',16,2022)",
               "  (48,'Zeph',26,2023)",
               "  (49,'Chadwick',14,2022)",
               "  (50,'Charlotte',17,2023)",
        	 	"(51, 'lee', 623, 2021)"};
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(8))";
         stmt.executeUpdate(s);
         System.out.println("Table DEPT created.");
         
         s = "create index idx_DID on DEPT(DId) using btree";
         stmt.executeUpdate(s);
         System.out.println("DEPT Index created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {"(10, 'compsci')",
                              "(623, 'math')",
                              "(623, 'drama')",
                              "(20, 'mathematics')",
                              "(30, 'engineering')",
         };
         for (int i=0; i<deptvals.length; i++)
            stmt.executeUpdate(s + deptvals[i]);
         System.out.println("DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         stmt.executeUpdate(s);
         System.out.println("Table COURSE created.");
         
         s = "create index idx_deptID on course(DeptID) using btree";
         stmt.executeUpdate(s);
         System.out.println("Course Index created.");
         
         s = "create index idx_CID on course(CId) using hash";
         stmt.executeUpdate(s);
         System.out.println("Course Index created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {"(13, 'db systems', 10)",
                                "(23, 'compilers', 10)",
                                "(34, 'calculus', 20)",
                                "(43, 'algebra', 20)",
                                "(53, 'acting', 30)",
                                "(623, 'elocution', 30)"};
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");
         

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");
         
         s = "create index idx_sectId on section(sectid) using hash";
         stmt.executeUpdate(s);
         System.out.println("sectID Index created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {"(14, 13, 'turing', 2018)",
                              "(24, 23, 'turing', 2019)",
                              "(34, 34, 'newton', 2019)",
                              "(44, 43, 'einstein', 2017)",
                              "(34, 53, 'brando', 2018)",
                              "(623, 53, 'brando', 2018)"};
         for (int i=0; i<sectvals.length; i++)
            stmt.executeUpdate(s + sectvals[i]);
         System.out.println("SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         stmt.executeUpdate(s);
         System.out.println("Table ENROLL created.");
         
         s = "create index idx_StudentId on enroll(studentId) using hash";
         stmt.executeUpdate(s);
         System.out.println("studentID Index created.");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {"(13, 1, 13, 'A')",
                                "(23, 1, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(43, 43, 34, 'B' )",
                                "(53, 42, 53, 'A' )",
                                "(23, 15, 13, 'C' )",
                                "(34, 26, 43, 'B+')",
                                "(43, 32, 23, 'B' )",
                                "(53, 14, 53, 'A' )",
                                "(23, 13, 43, 'C' )",
                                "(34, 26, 34, 'B+')",
                                "(43, 49, 34, 'B' )",
                                "(53, 48, 53, 'A' )",
                                "(23, 16, 43, 'C' )",
                                "(34, 23, 23, 'B+')",
                                "(43, 46, 34, 'B' )",
                                "(53, 42, 13, 'A' )",
                                "(23, 16, 43, 'C' )",
                                "(34, 22, 43, 'B+')",
                                "(43, 43, 34, 'B' )",
                                "(23, 23, 13, 'C' )",
                                "(34, 32, 43, 'B+')",
                                "(43, 43, 34, 'B' )",
                                "(53, 12, 13, 'A' )",
                                "(23, 5, 43, 'C' )",
                                "(34, 36, 43, 'B+')",
                                "(43, 22, 13, 'B' )",
                                "(53, 13, 53, 'A' )",
                                "(23, 11, 43, 'C' )",
                                "(34, 23, 43, 'B+')",
                                "(43, 48, 34, 'B' )",
                                "(53, 41, 53, 'A' )",
                                "(23, 13, 43, 'C' )",
                                "(34, 24, 43, 'B+')",
                                "(43, 42, 34, 'B' )",
                                "(53, 41, 53, 'A' )",
                                "(23, 32, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(43, 3, 34, 'B' )",
                                "(53, 4, 53, 'A' )",
                                "(623, 61, 53, 'A' )"};
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");
         
         
         //testing queries
         
//         SimpleDB db = new SimpleDB("studentdb");
//         Transaction tx = db.newTx();
//         Planner planner = db.planner();
//         String qry = "select sid, eid from student, enroll where sid = studentid";
//         
//         Plan p = planner.createQueryPlan(qry, tx);
//         Scan s1 = p.open();
//         int i = 0;
//         while (s1.next()) {
//           System.out.println(s1.getInt("sid") + " " + s1.getInt("eid"));
//         }
//         s1.close();
//         tx.commit();
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}

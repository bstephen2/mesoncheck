module sants;

import std.array : array;
import std.string;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_sants(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   ulong c_done;

   // dfmt off
   string sql_1 = "SELECT sants.pid FROM sants LEFT JOIN problem "
      				~ "ON sants.pid = problem.pid "
      				~ "WHERE problem.pid IS NULL "
      				~ "ORDER BY sants.pid";
      				
   string sql_2 = "SELECT sants.said FROM sants LEFT JOIN problem "
      				~ "ON sants.said = problem.pid "
      				~ "WHERE problem.pid IS NULL "
      				~ "ORDER BY sants.said";
      				
   string sql_2a =	"SELECT a.pid, a.said, a.years FROM sants AS a, problem AS b "
							~ "WHERE (a.said = b.pid) AND (a.years != b.years)"
							~ "ORDER BY a.pid, a.said";
							
   string sql_3 = "SELECT a.pid, a.said FROM sants AS a, nots AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.said = b.aid) "
      				~ "ORDER BY a.pid, a.said";
      
   string sql_3a	= "SELECT a.pid, a.said FROM sants AS a, vnots AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.said = b.aid) "
      				~ "ORDER BY a.pid, a.said";
      
   string sql_4 = "SELECT a.pid, a.said FROM sants AS a, sabs AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.said = b.sabid) "
      				~ "ORDER BY a.pid, a.said";
      
   string sql_5 = "SELECT a.pid, a.said FROM sants AS a, cants AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.said = b.caid) "
      				~ "ORDER BY a.pid, a.said";
      
   string sql_6 = "SELECT a.pid, a.said FROM sants AS a, cabs AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.said = b.cabid) "
      				~ "ORDER BY a.pid, a.said";
      
   string sql_7 = "CREATE TEMPORARY TABLE tempe SELECT pid FROM problemsource "
   					~ "WHERE sid = 5454 "
   					~ "ORDER BY pid";
   
   string sql_7a	=	"SELECT sants.pid FROM sants LEFT JOIN tempe "
      				~ "ON sants.pid = tempe.pid "
      				~ "WHERE tempe.pid IS NULL "
      				~ "ORDER BY sants.pid";
      
   string sql_8 	= "SELECT sants.said FROM sants LEFT JOIN tempe "
      				~ "ON sants.said = tempe.pid "
      				~ "WHERE tempe.pid IS NULL "
      				~ "ORDER BY sants.said";
      
   string sql_9 	= "SELECT sants.pid, sants.said FROM sants LEFT JOIN sabs "
   					~ "ON (sants.pid = sabs.sabid) AND (sants.said = sabs.pid) "
  						~ "WHERE sabs.pid IS NULL "
   					~ "ORDER BY sants.pid";
   
   string sql_9a	=	"SELECT sants.pid, sants.said, sants.years, sabs.years FROM sants, sabs "
   					~	"WHERE (sants.pid = sabs.sabid) AND (sants.said = sabs.pid) "
   					~	"ORDER BY sants.pid";
   
	// dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   
   /+	(1)	Check that every pid in table Sants is in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID " ~ to!string(row[0]) ~ " not in table Problem";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that every said in table Sants is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      string mess = "SAID " ~ to!string(row[0]) ~ " not in table Problem";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2a)	Check that every (said + years) in table Sants agrees with
	 +			(pid + years ) in table Problem.
	 +/

   range = conn.query(sql_2a);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID, YEARS ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ", "
      					~ to!string(row[2])
      					~ ") doesn't equal years in table Problem";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every (pid + said) in table Sants is not in table
	 +			Nots.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Nots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3a)	Check that every (pid + said) in table Sants is not in table
	 +			Vnots.
	 +/

   range = conn.query(sql_3a);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Vnots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + said) in table Sants is not in table
	 +			Sabs.
	 +/

   range = conn.query(sql_4);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Sabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that every (pid + said) in table Sants is not in table
	 +			Cants.
	 +/

   range = conn.query(sql_5);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Cants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + Said) in table Sants is not in table
	 +			Cabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Cabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check that every pid in table Sants is in table ProblemSource
	 +			as a near snap;
	 +/

   c_done = conn.exec(sql_7);

   range = conn.query(sql_7a);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID "
      					~ to!string(row[0])
      					~ " needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(8)	Check that every said in table Sants in in table ProblemSource
	 +			as a snap;
	 +/

   range = conn.query(sql_8);

   foreach (Row row; range) {
      // dfmt off
      string mess = "SAID "
      					~ to!string(row[0])
      					~ " needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9)	Check that every (pid + said) in table Sants is reciprocated
	 +			as (sabid + pid) in table Sabs.
	 +/

   range = conn.query(sql_9);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SAID ("
      				~ to!string(row[0])
      				~ ", "
      				~ to!string(row[1])
      				~ ") not reciprocated in table Sabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9a)	For Sants records that are reciprocated in Sabs, check that
	 +			the years make sense.
	 +/

   range = conn.query(sql_9a);

   foreach (Row row; range) {
      string sants_year = to!string(row[2]);
      string sabs_year = to!string(row[3]);

      if ((cmp(sants_year, "0000") != 0) && (cmp(sabs_year, "0000") != 0)) {
         if (cmp(sabs_year, sants_year) > 0) {

            // dfmt off
      		string mess = "PID, SAID ("
      						~ to!string(row[0])	
      						~	", "
      						~	to!string(row[1])
      						~	") years wrong way round in reciprocated record in table Sabs!";
      		// dfmt on
            log ~= mess;
            rc++;
         }
      }
   }

   range.close();

   conn.close();

   display_status(id, log, "check_sants");

   return rc;
}

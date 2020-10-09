module sabs;

import std.array : array;
import std.variant;
import std.conv;
import std.string;
import mysql;

import check;

uint check_sabs(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   ulong c_done;

   // dfmt off

    string sql_1	=	"SELECT sabs.pid FROM sabs LEFT JOIN problem "
      				~	"ON sabs.pid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY sabs.pid";
      				
    string sql_2	=	"SELECT sabs.sabid FROM sabs LEFT JOIN problem "
      				~	"ON sabs.sabid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY sabs.sabid";
      				
    string sql_2a	=	"SELECT a.pid, a.sabid, a.years FROM sabs AS a, problem AS b "
      				~	"WHERE (a.sabid = b.pid) AND (a.years != b.years)"
      				~	"ORDER BY a.pid, a.sabid";
      
    string sql_3	=	"SELECT a.pid, a.sabid FROM sabs AS a, sants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.sabid = b.said) "
      				~	"ORDER BY a.pid, a.sabid";
      
    string sql_4	=	"SELECT a.pid, a.sabid FROM sabs AS a, nots AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.sabid = b.aid) "
      				~	"ORDER BY a.pid, a.sabid";
      
    string sql_4a	=	"SELECT a.pid, a.sabid FROM sabs AS a, vnots AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.sabid = b.aid) "
      				~	"ORDER BY a.pid, a.sabid";
      
    string sql_5	=	"SELECT a.pid, a.sabid FROM sabs AS a, cants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.sabid = b.caid) "
      				~	"ORDER BY a.pid, a.sabid";
      
    string sql_6	=	"SELECT a.pid, a.sabid FROM sabs AS a, cabs AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.sabid = b.cabid) "
      				~	"ORDER BY a.pid, a.sabid";

    string sql_6a	=	"CREATE TEMPORARY TABLE sabtemp SELECT pid FROM problemsource "
   					~	"WHERE sid = 5454 "
   					~	"ORDER BY pid";
      				
    string sql_7	=	"SELECT sabs.pid FROM sabs LEFT JOIN sabtemp "
      				~	"ON sabs.pid = sabtemp.pid "
      				~	"WHERE sabtemp.pid IS NULL "
      				~	"ORDER BY sabs.pid";
      
    string sql_8	=	"SELECT sabs.sabid FROM sabs LEFT JOIN sabtemp "
      				~	"ON sabs.sabid = sabtemp.pid "
      				~	"WHERE sabtemp.pid IS NULL "
      				~	"ORDER BY sabs.sabid";
      
   string sql_9 	= "SELECT sabs.pid, sabs.sabid FROM sabs LEFT JOIN sants "
   					~ "ON (sabs.pid = sants.said) AND (sabs.sabid = sants.pid) "
  						~ "WHERE sants.pid IS NULL "
   					~ "ORDER BY sabs.pid";
   
   string sql_9a	=	"SELECT sabs.pid, sabs.sabid, sabs.years, sants.years FROM sabs, sants "
   					~	"WHERE (sabs.pid = sants.said) AND (sabs.sabid = sants.pid) "
   					~	"ORDER BY sabs.pid";
   
      // dfmt on

   /+	(1)	Check that every pid in table Sabs is in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      //dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" not in table Problem!";
      //dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that every sabid in table Sabs is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"SABID "
      				~	to!string(row[0])
      				~	" not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2a)	Check that every (sabid + years) in table Sabs agrees with
	 +			(pid + years ) in table Problem.
	 +/

   range = conn.query(sql_2a);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID, YEARS ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	", "
      				~	to!string(row[2])
      				~	") doesn't equal years in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every (pid + sabid) in table Sabs is not in table
	 +			Sants.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Sants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + sabid) in table Sabs is not in table
	 +			Nots.
	 +/
   range = conn.query(sql_4);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Nots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4a)	Check that every (pid + sabid) in table Sabs is not in table
	 +			Vnots.
	 +/
   range = conn.query(sql_4a);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Vnots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that every (pid + sabid) in table Sabs is not in table
	 +			Cants.
	 +/
   range = conn.query(sql_5);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID ( "
      				~	to!string(row[0])
      				~	", " ~ to!string(row[1])
      				~	") also in table Cants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + sabid) in table Sabs is not in table
	 +			Cabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, SABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Cabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check that every pid in table Sabs is in table ProblemSource
	 +			as a near snap;
	 +/

   c_done = conn.exec(sql_6a);

   version (NOTDELL) {
      range = conn.query(sql_7);

      foreach (Row row; range) {
         // dfmt off
      	string mess	=	"PID "
      					~	to!string(row[0])
      					~	" needs a NEAR SNAP!";
      	// dfmt on
         log ~= mess;
         rc++;
      }

      range.close();
   }

   /+	(8)	Chcck that every sabid in table Sabs is in table ProblemSource
	 +			as a near snap;
	 +/

   range = conn.query(sql_8);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"SABID "
      				~	to!string(row[0])
      				~	" needs a NEAR SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9)	Check that every (pid + sabid) in table Sabs is reciprocated
	 +			as (said + pid) in table Sants.
	 +/

   range = conn.query(sql_9);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, SABID ("
      				~ to!string(row[0])
      				~ ", "
      				~ to!string(row[1])
      				~ ") not reciprocated in table Sants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9a)	For Sabs records that are reciprocated in Sants, check that
	 +			the years make sense.
	 +/

   range = conn.query(sql_9a);

   foreach (Row row; range) {
      string sabs_year = to!string(row[2]);
      string sants_year = to!string(row[3]);

      if ((cmp(sants_year, "0000") != 0) && (cmp(sabs_year, "0000") != 0)) {
         if (cmp(sabs_year, sants_year) > 0) {

            // dfmt off
      		string mess = "PID, SABID ("
      						~ to!string(row[0])	
      						~	", "
      						~	to!string(row[1])
      						~	") years wrong way round in reciprocated record in table Sants!";
      		// dfmt on
            log ~= mess;
            rc++;
         }
      }
   }

   range.close();

   conn.close();

   display_status(id, log, "check_sabs");

   return rc;
}

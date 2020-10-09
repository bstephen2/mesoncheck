module cabs;

import std.array : array;
import std.variant;
import std.conv;
import std.string;
import mysql;

import check;

uint check_cabs(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   ulong c_done;

   // dfmt off

    string sql_1	=	"SELECT cabs.pid FROM cabs LEFT JOIN problem "
      				~	"ON cabs.pid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY cabs.pid";
      				
    string sql_2	=	"SELECT cabs.cabid FROM cabs LEFT JOIN problem "
      				~	"ON cabs.cabid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY cabs.cabid";
      				
    string sql_2a	=	"SELECT a.pid, a.cabid, a.years FROM cabs AS a, problem AS b "
      				~	"WHERE (a.cabid = b.pid) AND (a.years != b.years)"
      				~	"ORDER BY a.pid, a.cabid";
      
    string sql_3	=	"SELECT a.pid, a.cabid FROM cabs AS a, cants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.cabid = b.caid) "
      				~	"ORDER BY a.pid, a.cabid";
      
    string sql_4	=	"SELECT a.pid, a.cabid FROM cabs AS a, nots AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.cabid = b.aid) "
      				~	"ORDER BY a.pid, a.cabid";
      
    string sql_4a	=	"SELECT a.pid, a.cabid FROM cabs AS a, vnots AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.cabid = b.aid) "
      				~	"ORDER BY a.pid, a.cabid";
      
    string sql_5	=	"SELECT a.pid, a.cabid FROM cabs AS a, sants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.cabid = b.said) "
      				~	"ORDER BY a.pid, a.cabid";
      
    string sql_6	=	"SELECT a.pid, a.cabid FROM cabs AS a, sabs AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.cabid = b.sabid) "
      				~	"ORDER BY a.pid, a.cabid";

    string sql_6a	=	"CREATE TEMPORARY TABLE cabtemp SELECT pid FROM problemsource "
   					~	"WHERE sid = 3713 "
   					~	"ORDER BY pid";
      				
    string sql_7	=	"SELECT cabs.pid FROM cabs LEFT JOIN cabtemp "
      				~	"ON cabs.pid = cabtemp.pid "
      				~	"WHERE cabtemp.pid IS NULL "
      				~	"ORDER BY cabs.pid";
      
    string sql_8	=	"SELECT cabs.cabid FROM cabs LEFT JOIN cabtemp "
      				~	"ON cabs.cabid = cabtemp.pid "
      				~	"WHERE cabtemp.pid IS NULL "
      				~	"ORDER BY cabs.cabid";
      
   string sql_9 	= "SELECT cabs.pid, cabs.cabid FROM cabs LEFT JOIN cants "
   					~ "ON (cabs.pid = cants.caid) AND (cabs.cabid = cants.pid) "
  						~ "WHERE cants.pid IS NULL "
   					~ "ORDER BY cabs.pid";
   
   string sql_9a	=	"SELECT cabs.pid, cabs.cabid, cabs.years, cants.years FROM cabs, cants "
   					~	"WHERE (cabs.pid = cants.caid) AND (cabs.cabid = cants.pid) "
   					~	"ORDER BY cabs.pid";
   
      // dfmt on
   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that every pid in table Cabs is in table Problem.
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

   /+	(2)	Check that every cabid in table Cabs is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"CABID "
      				~	to!string(row[0])
      				~	" not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2a)	Check that every (cabid + years) in table Cabs agrees with
	 +			(pid + years ) in table Problem.
	 +/

   range = conn.query(sql_2a);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID, YEARS ( "
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

   /+	(3)	Check that every (pid + cabid) in table Cabs is not in table
	 +			Cants.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Cants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + cabid) in table Cabs is not in table
	 +			Nots.
	 +/
   range = conn.query(sql_4);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Nots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4a)	Check that every (pid + cabid) in table Cabs is not in table
	 +			Vnots.
	 +/
   range = conn.query(sql_4a);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Vnots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   /+	(5)	Check that every (pid + cabid) in table Cabs is not in table
	 +			Sants.
	 +/
   range = conn.query(sql_5);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID ( "
      				~	to!string(row[0])
      				~	", " ~ to!string(row[1])
      				~	") also in table Sants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + cabid) in table Cabs is not in table
	 +			Sabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID, CABID ( "
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	") also in table Sabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check that every pid in table Cabs is in table ProblemSource
	 +			as a snap;
	 +/

   c_done = conn.exec(sql_6a);

   range = conn.query(sql_7);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(8)	Chcck that every cabid in table Cabs is in table ProblemSource
	 +			as a snap;
	 +/
   range = conn.query(sql_8);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"CABID "
      				~	to!string(row[0])
      				~	" needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9)	Check that every (pid + cabid) in table Cabs is reciprocated
	 +			as (caid + pid) in table Cants.
	 +/

   range = conn.query(sql_9);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CABID ("
      				~ to!string(row[0])
      				~ ", "
      				~ to!string(row[1])
      				~ ") not reciprocated in table Cants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9a)	For Cabs records that are reciprocated in Cants, check that
	 +			the years make sense.
	 +/

   range = conn.query(sql_9a);

   foreach (Row row; range) {
      string cabs_year = to!string(row[2]);
      string cants_year = to!string(row[3]);

      if ((cmp(cants_year, "0000") != 0) && (cmp(cabs_year, "0000") != 0)) {
         if (cmp(cabs_year, cants_year) > 0) {

            // dfmt off
      		string mess = "PID, CABID ("
      						~ to!string(row[0])	
      						~	", "
      						~	to!string(row[1])
      						~	") years wrong way round in reciprocated record in table Cabs!";
      		// dfmt on
            log ~= mess;
            rc++;
         }
      }
   }

   range.close();

   conn.close();

   display_status(id, log, "check_cabs");

   return rc;
}

module problem_source;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_problem_source(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   ulong c_done;
   string sql_1 = "SELECT problemsource.pid FROM problemsource LEFT JOIN problem " ~ "ON problemsource.pid = problem.pid " ~ "WHERE problem.pid IS NULL " ~ "ORDER BY problemsource.pid";
   string sql_2 = "SELECT problemsource.sid FROM problemsource LEFT JOIN source " ~ "ON problemsource.sid = source.sid " ~ "WHERE source.sid IS NULL " ~ "ORDER BY problemsource.sid";
   string sql_3 = "CREATE TEMPORARY TABLE tempc SELECT a.pid FROM problemsource AS a LEFT JOIN cants AS b " ~ "ON a.pid = b.pid " ~ "WHERE (a.sid = 3713) AND (b.pid IS NULL) " ~ "ORDER BY a.pid";
   string sql_3a = "SELECT tempc.pid FROM tempc LEFT JOIN cabs "
      ~ "ON tempc.pid = cabs.pid "
      ~ "WHERE cabs.pid IS NULL " ~ "ORDER BY tempc.pid";
   string sql_4 = "CREATE TEMPORARY TABLE tempd SELECT a.pid FROM problemsource AS a LEFT JOIN sants AS b " ~ "ON a.pid = b.pid " ~ "WHERE (a.sid = 5454) AND (b.pid IS NULL) " ~ "ORDER BY a.pid";
   string sql_4a = "SELECT tempd.pid FROM tempd LEFT JOIN sabs "
      ~ "ON tempd.pid = sabs.pid "
      ~ "WHERE sabs.pid IS NULL " ~ "ORDER BY tempd.pid";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the PID of every record in table ProblemSource is
	 +			in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID " ~ to!string(row[0]) ~ " not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that the SID of every record in table ProblemSource is
	 +			in table Source.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      string mess = "SID " ~ to!string(row[0]) ~ " not in table Source!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every PID + SID(3713) in table ProblemSource has an
	 +			entry (PID) in table Cants or table Cabs.
	 +/

   c_done = conn.exec(sql_3);

   range = conn.query(sql_3a);

   foreach (Row row; range) {
      string mess = "Snapped PID " ~ to!string(row[0]) ~ " not in tables Cants or Cabs!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every PID + SID(5454) in table ProblemSource has an
	 +			entry (PID) in table Sants or table Sabs.
	 +/

   c_done = conn.exec(sql_4);

   range = conn.query(sql_4a);

   foreach (Row row; range) {
      string mess = "Near snapped PID " ~ to!string(row[0]) ~ " not in tables Sants or Sabs!";
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_problem_source");

   return rc;
}

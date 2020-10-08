module problem_composer;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_problem_composer(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   string sql_1 = "SELECT problemcomposer.pid FROM problemcomposer LEFT JOIN problem " ~ "ON problemcomposer.pid = problem.pid " ~ "WHERE problem.pid IS NULL " ~ "ORDER BY problemcomposer.pid";
   string sql_2 = "SELECT problemcomposer.cid FROM problemcomposer LEFT JOIN composer " ~ "ON problemcomposer.cid = composer.cid " ~ "WHERE composer.cid IS NULL " ~ "ORDER BY problemcomposer.cid";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the PID of every record in table ProblemComposer is
	 +			in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID " ~ to!string(row[0]) ~ " not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that the CID of every record in table ProblemComposer is
	 +			in table Composer.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      string mess = "CID " ~ to!string(row[0]) ~ " not in table Composer!";
      log ~= mess;
      rc++;
   }

   range.close();

   conn.close();

   display_status(id, log, "check_problem_composer");

   return rc;
}

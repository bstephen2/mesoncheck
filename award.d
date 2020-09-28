module award;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_award(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   string sql_1 = "SELECT award.aid, award.name FROM award LEFT JOIN problem "
      ~ "ON award.aid = problem.aid "
      ~ "WHERE problem.aid IS NULL " ~ "ORDER BY award.aid";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the AID of every record in table Award appears in
	 +			table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "AID " ~ to!string(row[0]) ~ "(" ~ to!string(row[1]) ~ ") not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_award");

   return rc;
}

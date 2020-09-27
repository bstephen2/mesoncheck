module versions;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_versions(uint id) {
   uint rc;
   string[] log;
   ResultRange range;
   Connection conn;
   string sql_1 = "SELECT versions.pid, versions.aid FROM versions LEFT JOIN problem " ~ "ON versions.pid = problem.eid AND versions.aid = problem.pid " ~ "WHERE problem.pid IS NULL " ~ "ORDER BY versions.pid, versions.aid";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that every entry in table Versions has a reciprocal
	 +			entry in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID AID (" ~ to!string(row[0]) ~ "," ~ to!string(
            row[1]) ~ ") not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_versions");

   return rc;
}

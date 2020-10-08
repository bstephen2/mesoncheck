module vnots;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_vnots(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   string sql_1 = "SELECT vnots.pid FROM vnots LEFT JOIN problem "
      ~ "ON vnots.pid = problem.pid "
      ~ "WHERE problem.pid IS NULL " ~ "ORDER BY vnots.pid";
   string sql_2 = "SELECT vnots.aid FROM vnots LEFT JOIN problem "
      ~ "ON vnots.aid = problem.pid "
      ~ "WHERE problem.pid IS NULL " ~ "ORDER BY vnots.aid";
   string sql_3 = "SELECT a.pid, a.aid FROM vnots AS a, cants AS b "
      ~ "WHERE (a.pid = b.pid) AND (a.aid = b.caid) " ~ "ORDER BY a.pid, a.aid";
   string sql_4 = "SELECT a.pid, a.aid FROM vnots AS a, cabs AS b "
      ~ "WHERE (a.pid = b.pid) AND (a.aid = b.cabid) " ~ "ORDER BY a.pid, a.aid";
   string sql_5 = "SELECT a.pid, a.aid FROM vnots AS a, sants AS b "
      ~ "WHERE (a.pid = b.pid) AND (a.aid = b.said) " ~ "ORDER BY a.pid, a.aid";
   string sql_6 = "SELECT a.pid, a.aid FROM vnots AS a, sabs AS b "
      ~ "WHERE (a.pid = b.pid) AND (a.aid = b.sabid) " ~ "ORDER BY a.pid, a.aid";
   string sql_7 = "SELECT count(*) AS rep, pid, aid FROM vnots "
      ~ "GROUP BY pid, aid " ~ "HAVING rep > 1";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that every pid in table vnots is in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID " ~ to!string(row[0]) ~ " not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that every aid in table vnots is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      string mess = "AID " ~ to!string(row[0]) ~ " not in table Problem!";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every (pid + aid) in table vnots is not in table
	 +			Cants.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
      string mess = "[" ~ to!string(row[0]) ~ ", " ~ to!string(row[1]) ~ "] also in table Cants";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + aid) in table vnots is not in table
	 +			Cabs.
	 +/

   range = conn.query(sql_4);

   foreach (Row row; range) {
      string mess = "[" ~ to!string(row[0]) ~ ", " ~ to!string(row[1]) ~ "] also in table Cabs";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that every (pid + aid) in table vnots is not in table
	 +			Sants.
	 +/

   range = conn.query(sql_5);

   foreach (Row row; range) {
      string mess = "[" ~ to!string(row[0]) ~ ", " ~ to!string(row[1]) ~ "] also in table Sants";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + aid) in table vnots is not in table
	 +			Sabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
      string mess = "[" ~ to!string(row[0]) ~ ", " ~ to!string(row[1]) ~ "] also in table Sabs";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check for duplicates.
	 +/

   range = conn.query(sql_7);

   foreach (Row row; range) {
      string mess = "[" ~ to!string(row[1]) ~ "," ~ to!string(
            row[2]) ~ "] duplicated " ~ to!string(row[0]) ~ " times";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(8)	Check that every pid,aid in table vnots is reciprocated as aid,pid.
	 +/

   conn.close();

   display_status(id, log, "check_vnots");

   return rc;
}

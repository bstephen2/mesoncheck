module composer;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_composer(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   string sql_1 = "SELECT composer.cid, composer.name FROM composer LEFT JOIN problemcomposer " ~ "ON composer.cid = problemcomposer.cid " ~ "WHERE problemcomposer.cid IS NULL " ~ "ORDER BY composer.cid";

   /+	(1)	Check that the CID of every record in table Composer is in table
	 +			ProblemComposer.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "CID " ~ to!string(row[0]) ~ "(" ~ to!string(
            row[1]) ~ ") not in table ProblemComposer!";
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_composer");

   return rc;
}

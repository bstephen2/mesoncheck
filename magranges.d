module magranges;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_magranges(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   string sql_1 = "SELECT jos_meson_magranges.mid FROM jos_meson_magranges LEFT JOIN source " ~ "ON jos_meson_magranges.mid = source.sid " ~ "WHERE source.sid IS NULL " ~ "ORDER BY jos_meson_magranges.mid";

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the MID of every record in table jos_meson_magranges appears
	 +			in table Source.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "MID " ~ to!string(row[0]) ~ " not in table Source!";
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_magranges");

   return rc;
}

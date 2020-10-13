module todoquote;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_todoquote(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   // dfmt off
   string sql_1	=	"SELECT todoquote.mid FROM todoquote LEFT JOIN source "
      				~	"ON todoquote.mid = source.sid "
      				~	"WHERE source.sid IS NULL "
      				~	"ORDER BY todoquote.mid";
   // dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the MID of every record in table todoquote appears
	 +			in table Source.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"MID "
      				~	to!string(row[0])
      				~	" not in table Source!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_todoquote");

   return rc;
}

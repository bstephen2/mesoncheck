module abstracted;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_abstracted(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+
	 +	(1)	Check that the SID of every record in table Abstracted appears
	 +			in table Source.
	 +/

   // dfmt off
   string sql_1	=	"SELECT abstracted.sid FROM abstracted LEFT JOIN source "
   					~	"ON abstracted.sid = source.sid "
   					~	"WHERE source.sid IS NULL "
   					~	"ORDER BY abstracted.sid";
   //dfmt on

   range = conn.query(sql_1);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"SID "
      				~	to!string(row[0])
      				~	" not in table Source!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_abstracted");

   return rc;
}

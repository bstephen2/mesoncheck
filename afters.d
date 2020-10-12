module afters;

import std.array : array;
import std.conv;
import std.variant;
import mysql;

import check;

uint check_afters(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   // dfmt off
   string sql_1	=	"SELECT afters.pid, afters.aid FROM afters LEFT JOIN problem "
   					~	"ON afters.pid = problem.eid AND afters.aid = problem.pid "
   					~	"WHERE problem.pid IS NULL "
   					~	"ORDER BY afters.pid, afters.aid";
   // dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+
	 +	(1)	Check that every entry in table Afters has a reciprocal entry
	 +			in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"PID AID ("
      				~	to!string(row[0])
      				~	","
      				~	to!string(row[1])
      				~	") not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_afters");

   return rc;
}

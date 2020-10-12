module classol;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_classol(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   // dfmt off
   string sql_1	=	"SELECT classol.pid FROM classol LEFT JOIN problem "
      				~	"ON classol.pid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY classol.pid";
   // dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the PID of every record in table Classol appears in table
	 +			Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_classol");

   return rc;
}

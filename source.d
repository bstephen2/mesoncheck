module source;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_source(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   ulong c_done;
   // dfmt off
   string sql_1	=	"CREATE TEMPORARY TABLE tempa SELECT source.sid, source.name FROM source LEFT JOIN problemsource "
   					~	"ON source.sid = problemsource.sid "
   					~	"WHERE problemsource.sid IS NULL "
   					~	"ORDER BY source.sid";
   
   string sql_2	=	"CREATE TEMPORARY TABLE tempb SELECT tempa.sid, tempa.name FROM tempa LEFT JOIN problem "
   					~	"ON tempa.sid = problem.sid "
   					~	"WHERE problem.sid IS NULL "
   					~	"ORDER BY tempa.sid";
   
   string sql_3	=	"SELECT tempb.sid, tempb.name FROM tempb LEFT JOIN jos_meson_magranges "
   					~	"ON tempb.sid = jos_meson_magranges.mid "
   					~	"WHERE jos_meson_magranges.mid IS NULL "
   					~	"ORDER BY tempb.sid";
   // dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the SID of every record in table Source is in table Problem or in table ProblemSource.
	 +/

   c_done = conn.exec(sql_1);
   c_done = conn.exec(sql_2);
   range = conn.query(sql_3);

   foreach (Row row; range) {
      // dfmt off
      string mess	=	"SID "
      				~	to!string(row[0])
      				~	"("
      				~	to!string(row[1])
         			~	") not in table ProblemSource, Problem or jos_meson_magranges!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();
   conn.close();

   display_status(id, log, "check_source");

   return rc;
}

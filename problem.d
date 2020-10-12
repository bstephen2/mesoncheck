module problem;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_problem(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;
   ulong c_done;
   //dfmt off
   string sql_1	=	"SELECT problem.pid FROM problem LEFT JOIN classol "
     					~	"ON problem.pid = classol.pid "
      				~	"WHERE classol.pid IS NULL "
      				~	"ORDER BY problem.pid";
      
   string sql_2	=	"SELECT problem.pid FROM problem LEFT JOIN problemcomposer "
      				~	"ON problem.pid = problemcomposer.pid "
      				~	"WHERE problemcomposer.pid IS NULL "
      				~	"ORDER BY problem.pid";
      
   string sql_3	=	"SELECT problem.sid FROM problem LEFT JOIN source "
      				~	"ON problem.sid = source.sid "
      				~	"WHERE source.sid IS NULL "
      				~	"ORDER BY problem.sid";
      
   string sql_4	=	"SELECT problem.aid FROM problem LEFT JOIN award "
      				~	"ON problem.aid = award.aid "
      				~	"WHERE award.aid IS NULL "
      				~	"ORDER BY problem.aid";
      
   string sql_5	=	"CREATE TEMPORARY TABLE tempb SELECT pid, eid FROM problem "
      				~	"WHERE eid != 0 "
      				~	"ORDER BY pid";
      
   string sql_5a	=	"SELECT tempb.pid, tempb.eid FROM tempb LEFT JOIN problem "
     					~	"ON tempb.eid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY tempb.pid";

   string sql_6	=	"SELECT problem.pid, problem.eid FROM problem LEFT JOIN versions "
   					~	"ON problem.eid = versions.pid AND problem.pid = versions.aid "
   					~	"WHERE (problem.version = 'Version') AND "
   					~	"(problem.eid IS NOT NULL) AND "
   					~	"(problem.eid != 0) AND "
   					~	"(versions.pid IS NULL) "
   					~	"ORDER BY problem.pid";

   string sql_7	=	"SELECT problem.pid, problem.eid FROM problem LEFT JOIN afters "
   					~	"ON problem.eid = afters.pid AND problem.pid = afters.aid "
   					~	"WHERE (problem.version = 'After') AND "
   					~	"(problem.eid IS NOT NULL) AND "
   					~	"(problem.eid != 0) AND "
   					~	"(afters.pid IS NULL) "
   					~	"ORDER BY problem.pid";
   // dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that the PID of every record in table Problem is in
	 +			table Classol.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" not in table Classol!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that the PID of every record in table Problem is in
	 +			table ProblemComposer.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" not in table ProblemComposer!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that the SID of every record in table Problem is in
	 +			table Source.
	 +/

   range = conn.query(sql_3);

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

   /+	(4)	Check that the AID of every record in table Problem is in
	 +			table Award.
	 +/

   range = conn.query(sql_4);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"AID "
      				~	to!string(row[0])
      				~	" not in table Award!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that the EID of every record in table Problem equals a
	 +			PID in table Problem.
	 +/

   c_done = conn.exec(sql_5);

   range = conn.query(sql_5a);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" (EID "
      				~	to!string(row[1])
      				~	") not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that for every 'version' in table Problem that there is
	 +			a reciprocal entry in table Versions.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" version EID "
      				~	to!string(row[1])
      				~	" not in table Versions!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check that for every 'after' in table Problem that there is a
	 +			reciprocal entry in table Afters.
	 +/

   range = conn.query(sql_7);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"PID "
      				~	to!string(row[0])
      				~	" after EID "
      				~	to!string(row[1])
      				~	" not in table Afters!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   conn.close();

   display_status(id, log, "check_problem");

   return rc;
}

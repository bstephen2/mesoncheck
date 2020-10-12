module nots;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import check;

uint check_nots(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   // dfmt off
   string sql_1	=	"SELECT nots.pid FROM nots LEFT JOIN problem "
      				~	"ON nots.pid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY nots.pid";
      
   string sql_2	=	"SELECT nots.aid FROM nots LEFT JOIN problem "
     					~	"ON nots.aid = problem.pid "
      				~	"WHERE problem.pid IS NULL "
      				~	"ORDER BY nots.aid";
      
   string sql_3	=	"SELECT a.pid, a.aid FROM nots AS a, cants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.aid = b.caid) "
      				~	"ORDER BY a.pid, a.aid";
      
   string sql_4	=	"SELECT a.pid, a.aid FROM nots AS a, cabs AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.aid = b.cabid) "
      				~	"ORDER BY a.pid, a.aid";
      
   string sql_5	=	"SELECT a.pid, a.aid FROM nots AS a, sants AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.aid = b.said) "
      				~	"ORDER BY a.pid, a.aid";
      
   string sql_6	=	"SELECT a.pid, a.aid FROM nots AS a, sabs AS b "
      				~	"WHERE (a.pid = b.pid) AND (a.aid = b.sabid) "
      				~	"ORDER BY a.pid, a.aid";
      
   string sql_7	=	"SELECT count(*) AS rep, pid, aid FROM nots "
      				~	"GROUP BY pid, aid "
      				~	"HAVING rep > 1";
   //dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that every pid in table nots is in table Problem.
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

   /+	(2)	Check that every aid in table nots is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"AID "
      				~	to!string(row[0])
      				~	" not in table Problem!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every (pid + aid) in table nots is not in table
	 +			Cants.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"["
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	"] also in table Cants";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + aid) in table nots is not in table
	 +			Cabs.
	 +/

   range = conn.query(sql_4);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"["
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	"] also in table Cabs";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that every (pid + aid) in table nots is not in table
	 +			Sants.
	 +/

   range = conn.query(sql_5);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"["
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	"] also in table Sants";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + aid) in table nots is not in table
	 +			Sabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"["
      				~	to!string(row[0])
      				~	", "
      				~	to!string(row[1])
      				~	"] also in table Sabs";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check for duplicates.
	 +/

   range = conn.query(sql_7);

   foreach (Row row; range) {
   	// dfmt off
      string mess	=	"["
      				~	to!string(row[1])
      				~	","
      				~	to!string(row[2])
      				~	"] duplicated "
      				~	to!string(row[0])
      				~	" times";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(8)	Check that every pid,aid in table nots is reciprocated as aid,pid.
	 +/

   conn.close();

   display_status(id, log, "check_nots");

   return rc;
}

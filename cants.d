module cants;

import std.array : array;
import std.variant;
import std.conv;
import std.string;
import mysql;

import constant;
import check;
import cant_rec;

uint check_cants(uint id) {
   uint rc;
   string[] log;
   cant_rec[] cantrecs;
   Connection conn;
   ResultRange range;
   ulong c_done;

   // dfmt off
   string sql_1 = "SELECT cants.pid FROM cants LEFT JOIN problem "
      				~ "ON cants.pid = problem.pid "
      				~ "WHERE problem.pid IS NULL "
      				~ "ORDER BY cants.pid";
      				
   string sql_2 = "SELECT cants.caid FROM cants LEFT JOIN problem "
      				~ "ON cants.caid = problem.pid "
      				~ "WHERE problem.pid IS NULL "
      				~ "ORDER BY cants.caid";
      				
   string sql_2a =	"SELECT a.pid, a.caid, a.years FROM cants AS a, problem AS b "
							~ "WHERE (a.caid = b.pid) AND (a.years != b.years)"
							~ "ORDER BY a.pid, a.caid";
							
   string sql_3 = "SELECT a.pid, a.caid FROM cants AS a, nots AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.caid = b.aid) "
      				~ "ORDER BY a.pid, a.caid";
      
   string sql_4 = "SELECT a.pid, a.caid FROM cants AS a, cabs AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.caid = b.cabid) "
      				~ "ORDER BY a.pid, a.caid";
      
   string sql_5 = "SELECT a.pid, a.caid FROM cants AS a, sants AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.caid = b.said) "
      				~ "ORDER BY a.pid, a.caid";
      
   string sql_6 = "SELECT a.pid, a.caid FROM cants AS a, sabs AS b "
      				~ "WHERE (a.pid = b.pid) AND (a.caid = b.sabid) "
      				~ "ORDER BY a.pid, a.caid";
      
   string sql_7 = "CREATE TEMPORARY TABLE tempe SELECT pid FROM problemsource "
   					~ "WHERE sid = 3713 "
   					~ "ORDER BY pid";
   
   string sql_7a	=	"SELECT cants.pid FROM cants LEFT JOIN tempe "
      					~ "ON cants.pid = tempe.pid "
      					~ "WHERE tempe.pid IS NULL "
      					~ "ORDER BY cants.pid";
      
   string sql_8 = "SELECT cants.caid FROM cants LEFT JOIN tempe "
      				~ "ON cants.caid = tempe.pid "
      				~ "WHERE tempe.pid IS NULL "
      				~ "ORDER BY cants.caid";
      
   string sql_9 = "SELECT pid, caid, years FROM cants";
   
   string sql_9a =	"SELECT years FROM cabs "
   						~ "WHERE (pid = %d) AND (cabid = %d)";
   
   string sql_10 =	"SELECT count(*) AS rep, pid, caid FROM cants "
      					~ "GROUP BY pid, caid "
      					~ "HAVING rep > 1";
	// dfmt on

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);

   /+	(1)	Check that every pid in table Cants is in table Problem.
	 +/

   range = conn.query(sql_1);

   foreach (Row row; range) {
      string mess = "PID " ~ to!string(row[0]) ~ " not in table Problem";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2)	Check that every aid in table Cants is in table Problem as pid.
	 +/

   range = conn.query(sql_2);

   foreach (Row row; range) {
      string mess = "CAID " ~ to!string(row[0]) ~ " not in table Problem";
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(2a)	Check that every (caid + years) in table Cants agrees with
	 +			(pid + years ) in table Problem.
	 +/

   range = conn.query(sql_2a);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CAID, YEARS ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ", "
      					~ to!string(row[2])
      					~ ") doesn't equal years in table Problem";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(3)	Check that every (pid + caid) in table Cants is not in table
	 +			Nots.
	 +/

   range = conn.query(sql_3);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Nots!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(4)	Check that every (pid + caid) in table Cants is not in table
	 +			Cabs.
	 +/

   range = conn.query(sql_4);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Cabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(5)	Check that every (pid + caid) in table Cants is not in table
	 +			Sants.
	 +/

   range = conn.query(sql_5);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Sants!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(6)	Check that every (pid + caid) in table Cants is not in table
	 +			Sabs.
	 +/

   range = conn.query(sql_6);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID, CAID ("
      					~ to!string(row[0])
      					~ ", "
      					~ to!string(row[1])
      					~ ") also in table Sabs!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(7)	Check that every pid in table Cants is in table ProblemSource
	 +			as a snap;
	 +/

   c_done = conn.exec(sql_7);

   range = conn.query(sql_7a);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID "
      					~ to!string(row[0])
      					~ " needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(8)	Check that every caid in table Cants in in table ProblemSource
	 +			as a snap;
	 +/

   range = conn.query(sql_8);

   foreach (Row row; range) {
      // dfmt off
      string mess = "CAID "
      					~ to!string(row[0])
      					~ " needs a SNAP!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   /+	(9)	Check that every (pid + caid) in table Cants is reciprocated
	 +			as (cabid + pid) in table Cabs with year order correct.
	 +/

   range = conn.query(sql_9);

   foreach (Row row; range) {
      int pid = row[0].get!(int);
      int caid = row[1].get!(int);
      string years = to!string(row[2]);

      cantrecs ~= new cant_rec(pid, caid, years);
   }

   range.close();

   foreach (cant_rec cr; cantrecs) {
      string nsql = format(sql_9a, cr.caid, cr.pid);
      log ~= nsql;
      rc++;
      //range = conn.query(nsql);
      //range.close();
   }

   /+

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid      = $r_row->[0];
        $caid     = $r_row->[1];
        $years    = $r_row->[2];
        $temp_sql = sprintf $ccan_check_9a, $caid, $pid;
        $sath     = $dbh->prepare($temp_sql);
        $sath->execute();
        if ( $r_arow = $sath->fetchrow_arrayref ) {
            $oyears = $r_arow->[0];
            if ( ( $years ne '0000' ) && ( $oyears ne '0000' ) ) {
                if ( $years lt $oyears ) {

                    $mess = sprintf
'PID, CAID (%d, %d) years wrong way round in reciprocated record in table Cabs!',
                      $pid, $caid;
                    $ar = [ $rid, 2, $mess ];
                    $message_queue->enqueue($ar);
                    $count++;
                }
            }
        }
        else {
            $mess =
              sprintf 'PID, CAID (%d, %d) not reciprocated in table Cabs!',
              $pid, $caid;
            $ar = [ $rid, 2, $mess ];
            $message_queue->enqueue($ar);

           $count++;
        }

    }

   	+/

   /+	(10)	Check for duplicates.
	 +/

   range = conn.query(sql_10);

   foreach (Row row; range) {
      // dfmt off
      string mess = "PID CAID ("
      					~ to!string(row[1])
      					~ ", "
      					~ to!string(row[2])
      					~ ") is duplicated!";
      // dfmt on
      log ~= mess;
      rc++;
   }

   range.close();

   conn.close();

   display_status(id, log, "check_cants");

   return rc;
}

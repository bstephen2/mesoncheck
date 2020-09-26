module problem_source;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_problem_source(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_problem_source");

   return rc;
}
/+
sub check_problem_source {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $sid;
    my $pid;
    my $ar;
    my $mess;
    my $cps_check_1 =
        'SELECT problemsource.pid FROM problemsource LEFT JOIN problem '
      . 'ON problemsource.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY problemsource.pid';
    my $cps_check_2 =
        'SELECT problemsource.sid FROM problemsource LEFT JOIN source '
      . 'ON problemsource.sid = source.sid '
      . 'WHERE source.sid IS NULL '
      . 'ORDER BY problemsource.sid';
    my $cps_check_3 =
'CREATE TEMPORARY TABLE tempc SELECT a.pid FROM problemsource AS a LEFT JOIN cants AS b '
      . 'ON a.pid = b.pid '
      . 'WHERE (a.sid = 3713) AND (b.pid IS NULL) '
      . 'ORDER BY a.pid';
    my $cps_check_3a =
        'SELECT tempc.pid FROM tempc LEFT JOIN cabs '
      . 'ON tempc.pid = cabs.pid '
      . 'WHERE cabs.pid IS NULL '
      . 'ORDER BY tempc.pid';
    my $cps_check_4 =
'CREATE TEMPORARY TABLE tempd SELECT a.pid FROM problemsource AS a LEFT JOIN sants AS b '
      . 'ON a.pid = b.pid '
      . 'WHERE (a.sid = 5454) AND (b.pid IS NULL) '
      . 'ORDER BY a.pid';
    my $cps_check_4a =
        'SELECT tempd.pid FROM tempd LEFT JOIN sabs '
      . 'ON tempd.pid = sabs.pid '
      . 'WHERE sabs.pid IS NULL '
      . 'ORDER BY tempd.pid';
    $ar = [ $rid, 1, 'Checking table ProblemSource ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table ProblemSource ...\n");

    #	(1)	Check that the PID of every record in table ProblemSource is
    #			in table Problem.

    $sth = $dbh->prepare($cps_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d not in table Problem!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print('\t\tPID $pid not in table Problem!\n');
        $count++;
    }

    $sth->finish();

    #	(2)	Check that the SID of every record in table ProblemSource is
    #			in table Source.

    $sth = $dbh->prepare($cps_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $sid  = $r_row->[0];
        $mess = sprintf 'SID %d not in table Source!', $sid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSID $sid not in table Source!\n");
        $count++;
    }

    $sth->finish();

    #	(3)	Check that every PID + SID(3713) in table ProblemSource has an
    #			entry (PID) in table Cants or table Cabs.

    $sth = $dbh->prepare($cps_check_3);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($cps_check_3a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'Snapped PID %d not in tables Cants or Cabs!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSnapped PID $pid not in tables Cants or Cabs!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every PID + SID(5454) in table ProblemSource has an
    #			entry (PID) in table Sants or table Sabs.

    $sth = $dbh->prepare($cps_check_4);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($cps_check_4a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'Near Snapped PID %d not in table Sants or Sabs!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tNear Snapped PID $pid not in tables Sants or Sabs!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

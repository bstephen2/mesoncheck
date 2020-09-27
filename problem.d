module problem;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_problem(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   conn.close();

   display_status(id, log, "check_problem");

   return rc;
}
/+
sub check_problem {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $pid;
    my $sid;
    my $aid;
    my $eid;
    my $ar;
    my $mess;
    my $cprob_check_1 =
        'SELECT problem.pid FROM problem LEFT JOIN classol '
      . 'ON problem.pid = classol.pid '
      . 'WHERE classol.pid IS NULL '
      . 'ORDER BY problem.pid';
    my $cprob_check_2 =
        'SELECT problem.pid FROM problem LEFT JOIN problemcomposer '
      . 'ON problem.pid = problemcomposer.pid '
      . 'WHERE problemcomposer.pid IS NULL '
      . 'ORDER BY problem.pid';
    my $cprob_check_3 =
        'SELECT problem.sid FROM problem LEFT JOIN source '
      . 'ON problem.sid = source.sid '
      . 'WHERE source.sid IS NULL '
      . 'ORDER BY problem.sid';
    my $cprob_check_4 =
        'SELECT problem.aid FROM problem LEFT JOIN award '
      . 'ON problem.aid = award.aid '
      . 'WHERE award.aid IS NULL '
      . 'ORDER BY problem.aid';
    my $cprob_check_5 =
        'CREATE TEMPORARY TABLE tempb SELECT pid, eid FROM problem '
      . 'WHERE eid != 0 '
      . 'ORDER BY pid';
    my $cprob_check_5a =
        'SELECT tempb.pid, tempb.eid FROM tempb LEFT JOIN problem '
      . 'ON tempb.eid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY tempb.pid';

    my $cprob_check_6 =
        'SELECT problem.pid, problem.eid FROM problem LEFT JOIN versions '
      . 'ON problem.eid = versions.pid AND problem.pid = versions.aid '
      . 'WHERE (problem.version = \'Version\') AND '
      . '(problem.eid IS NOT NULL) AND '
      . '(problem.eid != 0) AND '
      . '(versions.pid IS NULL) '
      . 'ORDER BY problem.pid';

    my $cprob_check_7 =
        'SELECT problem.pid, problem.eid FROM problem LEFT JOIN afters '
      . 'ON problem.eid = afters.pid AND problem.pid = afters.aid '
      . 'WHERE (problem.version = \'After\') AND '
      . '(problem.eid IS NOT NULL) AND '
      . '(problem.eid != 0) AND '
      . '(afters.pid IS NULL) '
      . 'ORDER BY problem.pid';

    $ar = [ $rid, 1, 'Checking table Problem ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Problem ...\n");

    #	(1)	Check that the PID of every record in table Problem is in
    #			table Classol.

    $sth = $dbh->prepare($cprob_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d not in table Classol!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid not in table Classol!\n");
        $count++;
    }

    $sth->finish();

    #	(2)	Check that the PID of every record in table Problem is in
    #			table ProblemComposer;

    $sth = $dbh->prepare($cprob_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d not in table ProblemComposer!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid not in table ProblemComposer!\n");
        $count++;
    }

    $sth->finish();

    #	(3)	Check that the SID of every record in table Problem is in
    #			table Source.

    $sth = $dbh->prepare($cprob_check_3);
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

    #	(4)	Check that the AID of every record in table Problem is in
    #			table Award.

    $sth = $dbh->prepare($cprob_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $aid  = $r_row->[0];
        $mess = sprintf 'AID %d not in table Award!', $aid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID $aid not in table Award!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that the EID of every record in table Problem equals a
    #			PID in table Problem.

    $sth = $dbh->prepare($cprob_check_5);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($cprob_check_5a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $eid  = $r_row->[1];
        $mess = sprintf 'PID %d (EID %d) not in table Problem!', $pid, $eid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid (EID $eid) not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that for every 'version' in table Problem that there is
    #			a reciprocal entry in table Versions.

    $sth = $dbh->prepare($cprob_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $eid  = $r_row->[1];
        $mess = sprintf 'PID %d version EID %d not in table Versions!', $pid,
          $eid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid version EID $eid not in table Versions!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check that for every 'after' in table Problem that there is a
    #			reciprocal entry in table Afters;

    $sth = $dbh->prepare($cprob_check_7);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $eid  = $r_row->[1];
        $mess = sprintf 'PID %d after EID %d not in table Afters!', $pid, $eid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid after EID $eid not in table Afters!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

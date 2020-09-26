module cabs;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_cabs(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_cabs");

   return rc;
}
/+
sub check_cabs {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $cabid;
    my $pid;
    my $years;
    my $ar;
    my $mess;
    my $ccab_check_1 =
        'SELECT cabs.pid FROM cabs LEFT JOIN problem '
      . 'ON cabs.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY cabs.pid';
    my $ccab_check_2 =
        'SELECT cabs.cabid FROM cabs LEFT JOIN problem '
      . 'ON cabs.cabid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY cabs.cabid';
    my $ccab_check_2a =
        'SELECT a.pid, a.cabid, a.years FROM cabs AS a, problem AS b '
      . 'WHERE (a.cabid = b.pid) AND (a.years != b.years)'
      . 'ORDER BY a.pid, a.cabid';
    my $ccab_check_3 =
        'SELECT a.pid, a.cabid FROM cabs AS a, cants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.cabid = b.caid) '
      . 'ORDER BY a.pid, a.cabid';
    my $ccab_check_4 =
        'SELECT a.pid, a.cabid FROM cabs AS a, nots AS b '
      . 'WHERE (a.pid = b.pid) AND (a.cabid = b.aid) '
      . 'ORDER BY a.pid, a.cabid';
    my $ccab_check_5 =
        'SELECT a.pid, a.cabid FROM cabs AS a, sants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.cabid = b.said) '
      . 'ORDER BY a.pid, a.cabid';
    my $ccab_check_6 =
        'SELECT a.pid, a.cabid FROM cabs AS a, sabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.cabid = b.sabid) '
      . 'ORDER BY a.pid, a.cabid';
    my $ccab_check_7 =
        'SELECT cabs.pid FROM cabs LEFT JOIN tempe '
      . 'ON cabs.pid = tempe.pid '
      . 'WHERE tempe.pid IS NULL '
      . 'ORDER BY cabs.pid';
    my $ccab_check_8 =
        'SELECT cabs.cabid FROM cabs LEFT JOIN tempe '
      . 'ON cabs.cabid = tempe.pid '
      . 'WHERE tempe.pid IS NULL '
      . 'ORDER BY cabs.cabid';
    my $ccab_check_9 = 'SELECT pid, cabid, years FROM cabs';
    my $ccab_check_9a =
      'SELECT years FROM cants ' . 'WHERE (pid = %d) AND (caid = %d)';
    my $ccab_check_10 =
        'SELECT count(*) AS rep, pid, cabid FROM cabs '
      . 'GROUP BY pid, cabid '
      . 'HAVING rep > 1';
    my $temp_sql;
    my $sath;
    my $r_arow;
    my $oyears;

    $ar = [ $rid, 1, 'Checking table Cabs ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Cabs ...\n");

    #	(1)	Check that every pid in table Cabs is in table Problem.

    $sth = $dbh->prepare($ccab_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d not in table Problem!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(2)	Check that every aid in table Cabs is in table Problem as pid.

    $sth = $dbh->prepare($ccab_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $cabid = $r_row->[0];
        $mess  = sprintf 'CABID %d not in table Problem!', $cabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCABID $cabid not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(2a)	Check that every (cabid + years) in table Cabs agrees with
    #			(pid + years ) in table Problem.

    $sth = $dbh->prepare($ccab_check_2a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $cabid = $r_row->[1];
        $years = $r_row->[2];
        $mess  = sprintf
'PID, CABID, YEARS (%d, %d, %s) doesn\'t equal years in table Problem!',
          $pid, $cabid, $years;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

#print("\t\tPID, CABID, YEAS ($pid, $cabid, $years) doesn't equal years in table Problem!\n");
        $count++;
    }

    #	(3)	Check that every (pid + cabid) in table Cabs is not in table
    #			Cants.

    $sth = $dbh->prepare($ccab_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $cabid = $r_row->[1];
        $mess  = sprintf 'PID, CABID (%d, %d) also in table Cants!', $pid,
          $cabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CABID ($pid, $cabid) also in table Cants!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every (pid + cabid) in table Cabs is not in table
    #			Nots.

    $sth = $dbh->prepare($ccab_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $cabid = $r_row->[1];
        $mess = sprintf 'PID, CABID (%d, %d) also in table Nots!', $pid, $cabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CABID ($pid, $cabid) also in table Nots!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that every (pid + cabid) in table Cabs is not in table
    #			Sants.

    $sth = $dbh->prepare($ccab_check_5);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $cabid = $r_row->[1];
        $mess  = sprintf 'PID, CABID (%d, %d) also int able Sants!', $pid,
          $cabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CABID ($pid, $cabid) also in table Sants!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that every (pid + cabid) in table Cabs is not in table
    #			Sabs.

    $sth = $dbh->prepare($ccab_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $cabid = $r_row->[1];
        $mess = sprintf 'PID, CABID (%d, %d) also in table Sabs!', $pid, $cabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CABID ($pid, $cabid) also in table Sabs!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check that every pid in table Cabs is in table ProblemSource
    #			as a snap;

    $sth = $dbh->prepare($ccab_check_7);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d needs a SNAP!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid needs a SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(8)	Chcck that every cabid in table Cabs in in table ProblemSource
    #			as a snap;

    $sth = $dbh->prepare($ccab_check_8);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $cabid = $r_row->[0];
        $mess  = sprintf 'CABID %d needs a SNAP!', $cabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCABID $cabid needs a SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(9)	Check that every (pid + cabid) in table Cabs is reciprocated
    #			as (caid + pid) in table Cants with year order correct.

    $sth = $dbh->prepare($ccab_check_9);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid      = $r_row->[0];
        $cabid    = $r_row->[1];
        $years    = $r_row->[2];
        $temp_sql = sprintf $ccab_check_9a, $cabid, $pid;
        $sath     = $dbh->prepare($temp_sql);
        $sath->execute();
        if ( $r_arow = $sath->fetchrow_arrayref ) {
            $oyears = $r_arow->[0];
            if ( ( $years ne '0000' ) && ( $oyears ne '0000' ) ) {
                if ( $years gt $oyears ) {
                    $mess = sprintf
'PID, CABID (%d, %d) years wrong way round in reciprocated record in table Cants!',
                      $pid, $cabid;
                    $ar = [ $rid, 2, $mess ];
                    $message_queue->enqueue($ar);

#print("\t\tPID, CABID ($pid, $cabid) years wrong way round in reciprocated record in table Cants!\n");
                    $count++;
                }
            }
        }
        else {
            $mess =
              sprintf 'PID, CABID (%d, %d) not reciprocated in table Cants',
              $pid, $cabid;
            $ar = [ $rid, 2, $mess ];
            $message_queue->enqueue($ar);

     #print("\t\tPID, CABID ($pid, $cabid) not reciprocated in table Cants!\n");
            $count++;
        }

        $sath->finish();
    }

    $sth->finish();

    #	(10) Check for duplicates.

    $sth = $dbh->prepare($ccab_check_10);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[1];
        $cabid = $r_row->[2];
        $mess  = sprintf 'PID, CABID (%d, %d) duplicated!', $pid, $cabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CABID ($pid, $cabid) duplicated!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

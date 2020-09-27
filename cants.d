module cants;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_cants(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   conn.close();

   display_status(id, log, "check_cants");

   return rc;
}
/+
sub check_cants {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $caid;
    my $pid;
    my $years;
    my $ar;
    my $mess;
    my $ccan_check_1 =
        'SELECT cants.pid FROM cants LEFT JOIN problem '
      . 'ON cants.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY cants.pid';
    my $ccan_check_2 =
        'SELECT cants.caid FROM cants LEFT JOIN problem '
      . 'ON cants.caid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY cants.caid';
    my $ccan_check_2a =
        'SELECT a.pid, a.caid, a.years FROM cants AS a, problem AS b '
      . 'WHERE (a.caid = b.pid) AND (a.years != b.years)'
      . 'ORDER BY a.pid, a.caid';
    my $ccan_check_3 =
        'SELECT a.pid, a.caid FROM cants AS a, nots AS b '
      . 'WHERE (a.pid = b.pid) AND (a.caid = b.aid) '
      . 'ORDER BY a.pid, a.caid';
    my $ccan_check_4 =
        'SELECT a.pid, a.caid FROM cants AS a, cabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.caid = b.cabid) '
      . 'ORDER BY a.pid, a.caid';
    my $ccan_check_5 =
        'SELECT a.pid, a.caid FROM cants AS a, sants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.caid = b.said) '
      . 'ORDER BY a.pid, a.caid';
    my $ccan_check_6 =
        'SELECT a.pid, a.caid FROM cants AS a, sabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.caid = b.sabid) '
      . 'ORDER BY a.pid, a.caid';
    my $ccan_check_7 =
        'CREATE TEMPORARY TABLE tempe SELECT pid FROM problemsource '
      . 'WHERE sid = 3713 '
      . 'ORDER BY pid';
    my $ccan_check_7a =
        'SELECT cants.pid FROM cants LEFT JOIN tempe '
      . 'ON cants.pid = tempe.pid '
      . 'WHERE tempe.pid IS NULL '
      . 'ORDER BY cants.pid';
    my $ccan_check_8 =
        'SELECT cants.caid FROM cants LEFT JOIN tempe '
      . 'ON cants.caid = tempe.pid '
      . 'WHERE tempe.pid IS NULL '
      . 'ORDER BY cants.caid';
    my $ccan_check_9 = 'SELECT pid, caid, years FROM cants';
    my $ccan_check_9a =
      'SELECT years FROM cabs ' . 'WHERE (pid = %d) AND (cabid = %d)';
    my $ccan_check_10 =
        'SELECT count(*) AS rep, pid, caid FROM cants '
      . 'GROUP BY pid, caid '
      . 'HAVING rep > 1';
    my $temp_sql;
    my $sath;
    my $r_arow;
    my $oyears;

    $ar = [ $rid, 1, 'Checking table Cants ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Cants ...\n");

    #	(1)	Check that every pid in table Cants is in table Problem.

    $sth = $dbh->prepare($ccan_check_1);
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

    #	(2)	Check that every aid in table Cants is in table Problem as pid.

    $sth = $dbh->prepare($ccan_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $caid = $r_row->[0];
        $mess = sprintf 'CAID %d not in table Problem!', $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCAID $caid not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(2a)	Check that every (caid + years) in table Cants agrees with
    #			(pid + years ) in table Problem.

    $sth = $dbh->prepare($ccan_check_2a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $caid  = $r_row->[1];
        $years = $r_row->[2];
        $mess  = sprintf
'PID, CAID, YEARS (%d, %d, %s) doesn\'t equal years in table Problem!',
          $pid, $caid, $years;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

#print("\t\tPID, CAID, YEAS ($pid, $caid, $years) doesn't equal years in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(3)	Check that every (pid + caid) in table Cants is not in table
    #			Nots.

    $sth = $dbh->prepare($ccan_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $caid = $r_row->[1];
        $mess = sprintf 'PID, CAID (%d, %d) also in table Nots!', $pid, $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CAID ($pid, $caid) also in table Nots!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every (pid + caid) in table Cants is not in table
    #			Cabs.

    $sth = $dbh->prepare($ccan_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $caid = $r_row->[1];
        $mess = sprintf 'PID, CAID (%d, %d) also in table Cabs!', $pid, $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CAID ($pid, $caid) also in table Cabs!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that every (pid + caid) in table Cants is not in table
    #			Sants.

    $sth = $dbh->prepare($ccan_check_5);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $caid = $r_row->[1];
        $mess = sprintf 'PID, CAID (%d, %d) also in table Sants!', $pid, $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CAID ($pid, $caid) also in table Sants!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that every (pid + caid) in table Cants is not in table
    #			Sabs.

    $sth = $dbh->prepare($ccan_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $caid = $r_row->[1];
        $mess = sprintf 'PID, CAID (%d, %d) also in table Sabs!', $pid, $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CAID ($pid, $caid) also in table Sabs!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check that every pid in table Cants is in table ProblemSource
    #			as a snap;

    $sth = $dbh->prepare($ccan_check_7);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($ccan_check_7a);
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

    #	(8)	Check that every caid in table Cants in in table ProblemSource
    #			as a snap;

    $sth = $dbh->prepare($ccan_check_8);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $caid = $r_row->[0];
        $mess = sprintf 'CAID %d needs a SNAP!', $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCAID $caid needs a SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(9)	Check that every (pid + caid) in table Cants is reciprocated
    #			as (cabid + pid) in table Cabs with year order correct.

    $sth = $dbh->prepare($ccan_check_9);
    $sth->execute();

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

#print("\t\tPID, CAID ($pid, $caid) years wrong way round in reciprocated record in table Cabs!\n");
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

        #print("\t\tPID, CAID ($pid, $caid) not reciprocated in table Cabs!\n");
            $count++;
        }

        $sath->finish();
    }

    $sth->finish();

    #	(10)	Check for duplicates.

    $sth = $dbh->prepare($ccan_check_10);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[1];
        $caid = $r_row->[2];
        $mess = sprintf 'PID, CAID (%d, %d) duplicated!', $pid, $caid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, CAID ($pid, $caid) duplicated!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    $count += check_cabs( $dbh, $rid + 2, $tid );

    return $count;
}
+/

module sants;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_sants(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_sants");

   return rc;
}
/+
sub check_sants {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $said;
    my $pid;
    my $years;
    my $ar;
    my $mess;
    my $csan_check_1 =
        'SELECT sants.pid FROM sants LEFT JOIN problem '
      . 'ON sants.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY sants.pid';
    my $csan_check_2 =
        'SELECT sants.said FROM sants LEFT JOIN problem '
      . 'ON sants.said = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY sants.said';
    my $csan_check_2a =
        'SELECT a.pid, a.said, a.years FROM sants AS a, problem AS b '
      . 'WHERE (a.said = b.pid) AND (a.years != b.years)'
      . 'ORDER BY a.pid, a.said';
    my $csan_check_3 =
        'SELECT a.pid, a.said FROM sants AS a, cants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.said = b.caid) '
      . 'ORDER BY a.pid, a.said';
    my $csan_check_4 =
        'SELECT a.pid, a.said FROM sants AS a, cabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.said = b.cabid) '
      . 'ORDER BY a.pid, a.said';
    my $csan_check_5 =
        'SELECT a.pid, a.said FROM sants AS a, nots AS b '
      . 'WHERE (a.pid = b.pid) AND (a.said = b.aid) '
      . 'ORDER BY a.pid, a.said';
    my $csan_check_6 =
        'SELECT a.pid, a.said FROM sants AS a, sabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.said = b.sabid) '
      . 'ORDER BY a.pid, a.said';
    my $csan_check_7 =
        'CREATE TEMPORARY TABLE tempf SELECT pid FROM problemsource '
      . 'WHERE sid = 5454 '
      . 'ORDER BY pid';
    my $csan_check_7a =
        'SELECT sants.pid FROM sants LEFT JOIN tempf '
      . 'ON sants.pid = tempf.pid '
      . 'WHERE tempf.pid IS NULL '
      . 'ORDER BY sants.pid';
    my $csan_check_8 =
        'SELECT sants.said FROM sants LEFT JOIN tempf '
      . 'ON sants.said = tempf.pid '
      . 'WHERE tempf.pid IS NULL '
      . 'ORDER BY sants.said';
    my $csan_check_9 = 'SELECT pid, said, years FROM sants';
    my $csan_check_9a =
      'SELECT years FROM sabs ' . 'WHERE (pid = %d) AND (sabid = %d)';
    my $csan_check_10 =
        'SELECT count(*) AS rep, pid, said FROM sants '
      . 'GROUP BY pid, said '
      . 'HAVING rep > 1';
    my $temp_sql;
    my $sath;
    my $r_arow;
    my $oyears;

    $ar = [ $rid, 1, 'Checking table Sants ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Sants ...\n");

    #	(1)	Check that every pid in table Sants is in table Problem.

    $sth = $dbh->prepare($csan_check_1);
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

    #	(2)	Check that every aid in table Sants is in table Problem as pid.

    $sth = $dbh->prepare($csan_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $said = $r_row->[0];
        $mess = sprintf 'SAID %d not in table Problem!', $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSAID $said not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(2a)	Check that every (said + years) in table Sants agrees with
    #			(pid + years ) in table Problem.

    $sth = $dbh->prepare($csan_check_2a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $said  = $r_row->[1];
        $years = $r_row->[2];
        $mess  = sprintf
'PID, SAID, YEARS (%d, %d, %s) doesn\'t equal years in table Problem!',
          $pid, $said, $years;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

#print("\t\tPID, SAID, YEAS ($pid, $said, $years) doesn't equal years in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(3)	Check that every (pid + said) in table Sants is not in table
    #			Cants.

    $sth = $dbh->prepare($csan_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $said = $r_row->[1];
        $mess = sprintf 'PID, SAID (%d, %d) also in table Cants!', $pid, $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) also in table Cants!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every (pid + said) in table Sants is not in table
    #			Cabs.

    $sth = $dbh->prepare($csan_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $said = $r_row->[1];
        $mess = sprintf 'PID, SAID (%d, %d) also in table Cabs!', $pid, $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) also in table Cabs!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that every (pid + said) in table Sants is not in table
    #			Nots.

    $sth = $dbh->prepare($csan_check_5);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $said = $r_row->[1];
        $mess = sprintf 'PID, SAID (%d, %d) also in table Nots!', $pid, $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) also in table Nots!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that every (pid + said) in table Sants is not in table
    #			Sabs.

    $sth = $dbh->prepare($csan_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $said = $r_row->[1];
        $mess = sprintf 'PID, SAID (%d, %d) also in table Sabs!', $pid, $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) also in table Sabs!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check that every pid in table Sants is in table ProblemSource
    #			as a near snap;

    $sth = $dbh->prepare($csan_check_7);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($csan_check_7a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $mess = sprintf 'PID %d needs a NEAR SNAP!', $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID $pid needs a NEAR SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(8)	Check that every said in table Sants in in table ProblemSource
    #			as a near snap;

    $sth = $dbh->prepare($csan_check_8);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $said = $r_row->[0];
        $mess = sprintf 'SAID %d needs a NEAR SNAP!', $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSAID $said needs a NEAR SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(9)	Check that every (pid + said) in table Sants is reciprocated
    #			as (sabid + pid) in table Sabs with year order correct.

    $sth = $dbh->prepare($csan_check_9);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid      = $r_row->[0];
        $said     = $r_row->[1];
        $years    = $r_row->[2];
        $temp_sql = sprintf $csan_check_9a, $said, $pid;
        $sath     = $dbh->prepare($temp_sql);
        $sath->execute();
        if ( $r_arow = $sath->fetchrow_arrayref ) {
            $oyears = $r_arow->[0];
            if ( ( $years ne '0000' ) && ( $oyears ne '0000' ) ) {
                if ( $years lt $oyears ) {
                    $mess = sprintf
'PID, SAID (%d, %d) years wrong way round in reciprocated record in table Sabs!',
                      $pid, $said;
                    $ar = [ $rid, 2, $mess ];
                    $message_queue->enqueue($ar);

#print("\t\tPID, SAID ($pid, $said) years wrong way round in reciprocated record in table Sabs!\n");
                    $count++;
                }
            }
        }
        else {
            $mess =
              sprintf 'PID, SAID (%d, %d) not reciprocated in table Sabs!',
              $pid, $said;
            $ar = [ $rid, 2, $mess ];
            $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) not reciprocated in table Sabs!\n");
            $count++;
        }

        $sath->finish();
    }

    $sth->finish();

    #	(10) Check for duplicates.

    $sth = $dbh->prepare($csan_check_10);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[1];
        $said = $r_row->[2];
        $mess = sprintf 'PID, SAID (%d, %d) duplicated!', $pid, $said;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SAID ($pid, $said) duplicated!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    $count += check_sabs( $dbh, $rid + 2, $tid );

    return $count;
}
+/

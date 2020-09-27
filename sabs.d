module sabs;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_sabs(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   conn.close();

   display_status(id, log, "check_sabs");

   return rc;
}
/+
sub check_sabs {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $sabid;
    my $pid;
    my $years;
    my $ar;
    my $mess;
    my $csab_check_1 =
        'SELECT sabs.pid FROM sabs LEFT JOIN problem '
      . 'ON sabs.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY sabs.pid';
    my $csab_check_2 =
        'SELECT sabs.sabid FROM sabs LEFT JOIN problem '
      . 'ON sabs.sabid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY sabs.sabid';
    my $csab_check_2a =
        'SELECT a.pid, a.Sabid, a.years FROM sabs AS a, problem AS b '
      . 'WHERE (a.sabid = b.pid) AND (a.years != b.years)'
      . 'ORDER BY a.pid, a.sabid';
    my $csab_check_3 =
        'SELECT a.pid, a.sabid FROM sabs AS a, cants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.sabid = b.caid) '
      . 'ORDER BY a.pid, a.sabid';
    my $csab_check_4 =
        'SELECT a.pid, a.sabid FROM sabs AS a, cabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.sabid = b.cabid) '
      . 'ORDER BY a.pid, a.sabid';
    my $csab_check_5 =
        'SELECT a.pid, a.sabid FROM sabs AS a, sants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.sabid = b.said) '
      . 'ORDER BY a.pid, a.sabid';
    my $csab_check_6 =
        'SELECT a.pid, a.sabid FROM sabs AS a, nots AS b '
      . 'WHERE (a.pid = b.pid) AND (a.sabid = b.aid) '
      . 'ORDER BY a.pid, a.sabid';
    my $csab_check_7 =
        'SELECT sabs.pid FROM sabs LEFT JOIN tempf '
      . 'ON sabs.pid = tempf.pid '
      . 'WHERE tempf.pid IS NULL '
      . 'ORDER BY sabs.pid';
    my $csab_check_8 =
        'SELECT sabs.sabid FROM sabs LEFT JOIN tempf '
      . 'ON sabs.sabid = tempf.pid '
      . 'WHERE tempf.pid IS NULL '
      . 'ORDER BY sabs.sabid';
    my $csab_check_9 = 'SELECT pid, sabid, years FROM sabs';
    my $csab_check_9a =
      'SELECT years FROM sants ' . 'WHERE (pid = %d) AND (said = %d)';
    my $csab_check_10 =
        'SELECT count(*) AS rep, pid, sabid FROM sabs '
      . 'GROUP BY pid, sabid '
      . 'HAVING rep > 1';
    my $temp_sql;
    my $sath;
    my $r_arow;
    my $oyears;

    $ar = [ $rid, 1, 'Checking table Sabs ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Sabs ...\n");

    #	(1)	Check that every pid in table Sabs is in table Problem.

    $sth = $dbh->prepare($csab_check_1);
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

    #	(2)	Check that every aid in table Sabs is in table Problem as pid.

    $sth = $dbh->prepare($csab_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $sabid = $r_row->[0];
        $mess  = sprintf 'SABID %d not in table Problem!', $sabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSABID $sabid not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(2a)	Check that every (sabid + years) in table Sabs agrees with
    #			(pid + years ) in table Problem.

    $sth = $dbh->prepare($csab_check_2a);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $sabid = $r_row->[1];
        $years = $r_row->[2];
        $mess  = sprintf
'PID, SABID, YEARS (%d, %d, %s) doesn\'t equal years in table Problem!',
          $pid, $sabid, $years;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

#print("\t\tPID, SABID, YEARS ($pid, $sabid, $years) doesn't equal years in table Problem!\n");
        $count++;
    }

    #	(3)	Check that every (pid + sabid) in table Sabs is not in table Cants.

    $sth = $dbh->prepare($csab_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $sabid = $r_row->[1];
        $mess  = sprintf 'PID, SABID (%d, %d) also in table Cants!', $pid,
          $sabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SABID ($pid, $sabid) also in table Cants!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every (pid + sabid) in table Sabs is not in table
    #			Cabs.

    $sth = $dbh->prepare($csab_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $sabid = $r_row->[1];
        $mess = sprintf 'PID, SABID (%d, %d) also in table Cabs!', $pid, $sabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SABID ($pid, $sabid) also in table Cabs!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that every (pid + sabid) in table Sabs is not in table
    #			Sants.

    $sth = $dbh->prepare($csab_check_5);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $sabid = $r_row->[1];
        $mess  = sprintf 'PID, SABID (%d, %d) also in table Sants!', $pid,
          $sabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SABID ($pid, $sabid) also in table Sants!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that every (pid + sabid) in table Sabs is not in table
    #			Nots.

    $sth = $dbh->prepare($csab_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[0];
        $sabid = $r_row->[1];
        $mess = sprintf 'PID, SABID (%d, %d) also in table Nots!', $pid, $sabid;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SABID ($pid, $sabid) also in table Nots!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check that every pid in table Sabs is in table ProblemSource
    #			as a near snap;

    $sth = $dbh->prepare($csab_check_7);
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

    #	(8)	Check that every sabid in table Sabs in in table ProblemSource
    #			as a near snap;

    $sth = $dbh->prepare($csab_check_8);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $sabid = $r_row->[0];
        $mess  = sprintf 'SABID %d needs a NEAR SNAP!', $sabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tSABID $sabid needs a NEAR SNAP!\n");
        $count++;
    }

    $sth->finish();

    #	(9)	Check that every (pid + sabid) in table Sabs is reciprocated
    #			as (said + pid) in table Sants with year order correct.

    $sth = $dbh->prepare($csab_check_9);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid      = $r_row->[0];
        $sabid    = $r_row->[1];
        $years    = $r_row->[2];
        $temp_sql = sprintf $csab_check_9a, $sabid, $pid;
        $sath     = $dbh->prepare($temp_sql);
        $sath->execute();
        if ( $r_arow = $sath->fetchrow_arrayref ) {
            $oyears = $r_arow->[0];
            if ( ( $years ne '0000' ) && ( $oyears ne '0000' ) ) {
                if ( $years gt $oyears ) {
                    $mess = sprintf
'PID, SABID (%d, %d) years wrong way round in reciprocated record in table Sants!',
                      $pid, $sabid;
                    $ar = [ $rid, 2, $mess ];
                    $message_queue->enqueue($ar);

#print("\t\tPID, SABID ($pid, $sabid) years wrong way round in reciprocated record in table Sants!\n");
                    $count++;
                }
            }
        }
        else {
            $mess =
              sprintf 'PID, SABID (%d, %d) not reciprocated in table Sants!',
              $pid, $sabid;
            $ar = [ $rid, 2, $mess ];
            $message_queue->enqueue($ar);

     #print("\t\tPID, SABID ($pid, $sabid) not reciprocated in table Sants!\n");
            $count++;
        }

        $sath->finish();
    }

    $sth->finish();

    #	(10) Check for duplicates.

    $sth = $dbh->prepare($csab_check_10);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid   = $r_row->[1];
        $sabid = $r_row->[2];
        $mess  = sprintf 'PID, SABID (%d, %d) duplicated!', $pid, $sabid;
        $ar    = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, SABID ($pid, $sabid) duplicated!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

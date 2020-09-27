module nots;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_nots(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   conn.close();

   display_status(id, log, "check_nots");

   return rc;
}
/+
sub check_nots {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $aid;
    my $pid;
    my $ar;
    my $mess;
    my $cnot_check_1 =
        'SELECT nots.pid FROM nots LEFT JOIN problem '
      . 'ON nots.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY nots.pid';
    my $cnot_check_2 =
        'SELECT nots.aid FROM nots LEFT JOIN problem '
      . 'ON nots.aid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY nots.aid';
    my $cnot_check_3 =
        'SELECT a.pid, a.aid FROM nots AS a, cants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.aid = b.caid) '
      . 'ORDER BY a.pid, a.aid';
    my $cnot_check_4 =
        'SELECT a.pid, a.aid FROM nots AS a, cabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.aid = b.cabid) '
      . 'ORDER BY a.pid, a.aid';
    my $cnot_check_5 =
        'SELECT a.pid, a.aid FROM nots AS a, sants AS b '
      . 'WHERE (a.pid = b.pid) AND (a.aid = b.said) '
      . 'ORDER BY a.pid, a.aid';
    my $cnot_check_6 =
        'SELECT a.pid, a.aid FROM nots AS a, sabs AS b '
      . 'WHERE (a.pid = b.pid) AND (a.aid = b.sabid) '
      . 'ORDER BY a.pid, a.aid';
    my $cnot_check_7 =
        'SELECT count(*) AS rep, pid, aid FROM nots '
      . 'GROUP BY pid, aid '
      . 'HAVING rep > 1';

    $ar = [ $rid, 1, 'Checking table Nots ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Nots ...\n");

    #	(1)	Check that every pid in table Nots is in table Problem.

    $sth = $dbh->prepare($cnot_check_1);
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

    #	(2)	Check that every aid in table Nots is in table Problem as pid.

    $sth = $dbh->prepare($cnot_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $aid  = $r_row->[0];
        $mess = sprintf 'AID %d not in table Problem!', $aid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID $aid not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    #	(3)	Check that every (pid + aid) in table Nots is not in table
    #			Cants.

    $sth = $dbh->prepare($cnot_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $aid  = $r_row->[1];
        $mess = sprintf 'PID, AID (%d, %d) also in table Cants!', $pid, $aid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, AID ($pid, $aid) also in table Cants!\n");
        $count++;
    }

    $sth->finish();

    #	(4)	Check that every (pid + aid) in table Nots is not in table Cabs.

    $sth = $dbh->prepare($cnot_check_4);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $aid  = $r_row->[1];
        $mess = sprintf 'AID, PID (%d, %d) also in table Cabs!', $aid, $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID, PID ($aid, $pid) also in table Cabs!\n");
        $count++;
    }

    $sth->finish();

    #	(5)	Check that every (pid + aid) in table Nots is not in table
    #			Sants.

    $sth = $dbh->prepare($cnot_check_5);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $aid  = $r_row->[1];
        $mess = sprintf 'AID, PID (%d, %d) also in table Sants!', $aid, $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID, PID ($aid, $pid) also in table Sants!\n");
        $count++;
    }

    $sth->finish();

    #	(6)	Check that every (pid + aid) in table Nots is not in table Sabs.

    $sth = $dbh->prepare($cnot_check_6);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $aid  = $r_row->[1];
        $mess = sprintf 'AID, PID (%d, %d) also in table Sabs!', $aid, $pid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID, PID ($aid, $pid) also in table Sabs!\n");
        $count++;
    }

    $sth->finish();

    #	(7)	Check for duplicates.

    $sth = $dbh->prepare($cnot_check_7);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[1];
        $aid  = $r_row->[2];
        $mess = sprintf 'PID, AID (%d, %d) duplicated!', $pid, $aid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID, AID ($pid, $aid) duplicated!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

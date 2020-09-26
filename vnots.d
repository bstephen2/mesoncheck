module vnots;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_vnots(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_vnots");

   return rc;
}
/+
sub check_vnots {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $aid;
    my $pid;
    my $ar;
    my $mess;
    my $cnot_check_1 =
        'SELECT vnots.pid FROM vnots LEFT JOIN problem '
      . 'ON vnots.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY vnots.pid';
    my $cnot_check_2 =
        'SELECT vnots.aid FROM vnots LEFT JOIN problem '
      . 'ON vnots.aid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY vnots.aid';
    my $cnot_check_7 =
        'SELECT count(*) AS rep, pid, aid FROM vnots '
      . 'GROUP BY pid, aid '
      . 'HAVING rep > 1';

    $ar = [ $rid, 1, 'Checking table Vnots ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Nots ...\n");

    #	(1)	Check that every pid in table vnots is in table Problem.

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

    #	(2)	Check that every aid in table vnots is in table Problem as pid.

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

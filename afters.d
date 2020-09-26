module afters;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_afters(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_afters");

   return rc;
}
/+
sub check_afters {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $pid;
    my $aid;
    my $ar;
    my $mess;
    my $caf_check_1 =
        'SELECT afters.pid, afters.aid FROM afters LEFT JOIN problem '
      . 'ON afters.pid = problem.eid AND afters.aid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY afters.pid, afters.aid';

    $ar = [ $rid, 1, 'Checking table Afters ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Afters ...\n");

    #	(1)	Check that every entry in table Afters has a reciprocal entry
    #			in table Problem.

    $sth = $dbh->prepare($caf_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $pid  = $r_row->[0];
        $aid  = $r_row->[1];
        $mess = sprintf 'PID AID (%d, %d) not in table Problem!', $pid, $aid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tPID AID ($pid, $aid) not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

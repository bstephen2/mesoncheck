module versions;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_versions(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_versions");

   return rc;
}
/+
sub check_versions {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $pid;
    my $aid;
    my $ar;
    my $mess;
    my $cv_check_1 =
        'SELECT versions.pid, versions.aid FROM versions LEFT JOIN problem '
      . 'ON versions.pid = problem.eid AND versions.aid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY versions.pid, versions.aid';

    $ar = [ $rid, 1, 'Checking table Versions ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Versions ...\n");

    #	(1)	Check that every entry in table Versions has a reciprocal
    #			entry in table Problem.

    $sth = $dbh->prepare($cv_check_1);
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

module award;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_award(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_award");

   return rc;
}
/+
sub check_award {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $aid;
    my $name;
    my $ar;
    my $mess;
    my $caw_check_1 =
        'SELECT award.aid, award.name FROM award LEFT JOIN problem '
      . 'ON award.aid = problem.aid '
      . 'WHERE problem.aid IS NULL '
      . 'ORDER BY award.aid';

    $ar = [ $rid, 1, 'Checking table Award ...', $tid ];
    $message_queue->enqueue($ar);

    #	(1)	Check that the AID of every record in table Award appears in
    #			table Problem.

    $sth = $dbh->prepare($caw_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $aid  = $r_row->[0];
        $name = $r_row->[1];
        $mess = sprintf 'AID %d (%s) not in table Problem!', $aid, $name;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tAID $aid ($name) not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

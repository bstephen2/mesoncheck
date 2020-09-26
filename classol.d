module classol;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_classol(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_classol");

   return rc;
}
/+
sub check_classol {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $pid;
    my $ar;
    my $mess;
    my $ccsol_check_1 =
        'SELECT classol.pid FROM classol LEFT JOIN problem '
      . 'ON classol.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY classol.pid';

    $ar = [ $rid, 1, 'Checking table Classol ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Classol ...\n");

    #	Check that the PID of every record in table Classol appears in table
    #	Problem.

    $sth = $dbh->prepare($ccsol_check_1);
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

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

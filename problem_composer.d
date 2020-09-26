module problem_composer;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_problem_composer(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_problem_composer");

   return rc;
}
/+
sub check_problem_composer {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $cid;
    my $pid;
    my $ar;
    my $mess;
    my $cpc_check_1 =
        'SELECT problemcomposer.pid FROM problemcomposer LEFT JOIN problem '
      . 'ON problemcomposer.pid = problem.pid '
      . 'WHERE problem.pid IS NULL '
      . 'ORDER BY problemcomposer.pid';
    my $cpc_check_2 =
        'SELECT problemcomposer.cid FROM problemcomposer LEFT JOIN composer '
      . 'ON problemcomposer.cid = composer.cid '
      . 'WHERE composer.cid IS NULL '
      . 'ORDER BY problemcomposer.cid';

    $ar = [ $rid, 1, 'Checking table Problemcomposer ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table ProblemComposer ...\n");

    #	(1)	Check that the PID of every record in table ProblemComposer is
    #			in table Problem.

    $sth = $dbh->prepare($cpc_check_1);
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

    #	(2)	Check that the CID of every record in table ProblemComposer is
    #			in table Composer.

    $sth = $dbh->prepare($cpc_check_2);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $cid  = $r_row->[0];
        $mess = sprintf 'CID %d not in table Composer!', $cid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCID $cid not in table Composer!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

module composer;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_composer(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_composer");

   return rc;
}
/+
sub check_composer {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $cid;
    my $name;
    my $ar;
    my $mess;
    my $ccomp_check_1 =
'SELECT composer.cid, composer.name FROM composer LEFT JOIN problemcomposer '
      . 'ON composer.cid = problemcomposer.cid '
      . 'WHERE problemcomposer.cid IS NULL '
      . 'ORDER BY composer.cid';

    $ar = [ $rid, 1, 'Checking table Composer ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Composer ...\n");

    #	Check that the CID of every record in table Composer is in table
    #	ProblemComposer.

    $sth = $dbh->prepare($ccomp_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $cid  = $r_row->[0];
        $name = $r_row->[1];
        $mess = sprintf 'CID %d (%s) not in table ProblemComposer!', $cid,
          $name;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

        #print("\t\tCID $cid ($name) not in table ProblemComposer!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

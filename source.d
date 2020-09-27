module source;

import std.array : array;
import std.variant;
import std.conv;
import mysql;

import constant;
import check;

uint check_source(uint id) {
   uint rc;
   string[] log;
   Connection conn;
   ResultRange range;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   conn.close();

   display_status(id, log, "check_source");

   return rc;
}
/+
sub check_source {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $sid;
    my $name;
    my $ar;
    my $mess;
    my $csrc_check_1 =
'CREATE TEMPORARY TABLE tempa SELECT source.sid, source.name FROM source LEFT JOIN problemsource '
      . 'ON source.sid = problemsource.sid '
      . 'WHERE problemsource.sid IS NULL '
      . 'ORDER BY source.sid';
    my $csrc_check_2 =
'CREATE TEMPORARY TABLE tempb SELECT tempa.sid, tempa.name FROM tempa LEFT JOIN problem '
      . 'ON tempa.sid = problem.sid '
      . 'WHERE problem.sid IS NULL '
      . 'ORDER BY tempa.sid';
    my $csrc_check_3 =
        'SELECT tempb.sid, tempb.name FROM tempb LEFT JOIN jos_meson_magranges '
      . 'ON tempb.sid = jos_meson_magranges.mid '
      . 'WHERE jos_meson_magranges.mid IS NULL '
      . 'ORDER BY tempb.sid';

    $ar = [ $rid, 1, 'Checking table Source ...', $tid ];
    $message_queue->enqueue($ar);

    #print("\tChecking table Source ...\n");

#	Check that the SID of every record in table Source is in table Problem or in table ProblemSource.

    $sth = $dbh->prepare($csrc_check_1);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($csrc_check_2);
    $sth->execute();
    $sth->finish();

    $sth = $dbh->prepare($csrc_check_3);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $sid  = $r_row->[0];
        $name = $r_row->[1];
        $mess = sprintf
'SID %d (%s) not in table ProblemSource, Problem or jos_meson_magranges!',
          $sid, $name;
        $ar = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);

#print("\t\tSID $sid ($name) not in table ProblemSource and not in table Problem!\n");
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

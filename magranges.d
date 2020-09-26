module magranges;

import std.array : array;
import std.variant;
import mysql;

import constant;
import check;

uint check_magranges(uint id) {
   uint rc;
   string[] log;
   Connection conn;

   auto connectionStr = "host=localhost;port=3306;user=bstephen;pwd=rice37;db=meson";
   conn = new Connection(connectionStr);
   scope (exit)
      conn.close();

   display_status(id, log, "check_magranges");

   return rc;
}
/+
sub check_jos_meson_magranges {
    my ( $dbh, $rid, $tid ) = @_;
    my $count = 0;
    my $sth;
    my $r_row;
    my $sid;
    my $ar;
    my $mess;
    my $cjmm_check_1 =
'SELECT jos_meson_magranges.mid FROM jos_meson_magranges LEFT JOIN source '
      . 'ON jos_meson_magranges.mid = source.sid '
      . 'WHERE source.sid IS NULL '
      . 'ORDER BY jos_meson_magranges.mid';

    $ar = [ $rid, 1, 'Checking table jos_meson_magranges ...', $tid ];
    $message_queue->enqueue($ar);

   #	(1)	Check that the SID of every record in table jos_meson_magranges appears
   #			in table Source.

    $sth = $dbh->prepare($cjmm_check_1);
    $sth->execute();

    while ( $r_row = $sth->fetchrow_arrayref ) {
        $sid  = $r_row->[0];
        $mess = sprintf 'SID %d not in table Source!', $sid;
        $ar   = [ $rid, 2, $mess ];
        $message_queue->enqueue($ar);
        $count++;
    }

    $sth->finish();

    $ar = [ $rid, 0 ];
    $message_queue->enqueue($ar);

    return $count;
}
+/

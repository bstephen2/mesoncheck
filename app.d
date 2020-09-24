module app;

/*
 *	A program to check the integrity of the Meson database.
 *
 *	(c) 2020, Brian Stephenson
 *	brian@bstephen.me.uk
 *
 *	USAGE: mesoncheck
 *	
 *	This program is threaded. It runs several routines, each making tests of certain MYSQL tables. Each routine
 * writes a log which is picked up by another thread, which, when all output from a routine is received, writes it
 *	to standard output. The program ends by printing a count of database errors encountered and the time taken.
 */

import std.stdio;
import std.datetime.stopwatch;
import std.getopt;
import check;

enum string prog_name = "mesoncheck";
enum string prog_version = "1.0";
enum string prog_author = "Brian Stephenson";
enum string prog_year = "2020";
enum double time_divisor = 10_000_000.0;

void main(string[] args) {
	uint rc = 0;
   uint error_count;
   uint thrs = 2;
   auto sw = StopWatch(AutoStart.yes);
   writefln("%s (v. %s)", prog_name, prog_version);
   writefln("(c) %s, %s", prog_year, prog_author);

   try {
      auto arginfo = getopt(args, "threads", &thrs);
   }
   catch (Exception e) {
      writefln("\n%s\n", e.msg);
   }

   if (thrs > 3)
   {
   	thrs = 3;
   	writeln("\nERROR: Number of threads capped at 3\n");
   }
   
   writefln("\t--threads = %d\n", thrs);
   
   

   error_count = check_database(thrs);

   sw.stop();
   auto seconds = sw.peek().total!"hnsecs" / time_divisor;
   writefln("\n%s finished (%d errors) in %3.6f secs.", prog_name, error_count, seconds);

   return;
}

module check;

import std.parallelism;
import core.atomic;
import std.stdio;
import std.process;

import abstracted;
import award;
import classol;
import composer;
import problem;
import problem_composer;
import problem_source;
import source;
import versions;
import afters;
import magranges;
import todoquote;
import nots;
import cants;
import cabs;
import sants;
import sabs;
import vnots;
import constant;

shared uint[ulong] tids;
shared uint newtid;

void display_status(uint id, string[] log, string name) {

   synchronized {
      ulong ltid = thisThreadID;
      uint* ptr = cast(uint*)(ltid in tids);

      if (ptr is null) {
         core.atomic.atomicOp!"+="(newtid, 1);
         tids[ltid] = newtid;
         ptr = cast(uint*)&newtid;
      }

      writefln("%2d) %2d ==> %s\n", *ptr, id, name);

      foreach (ref str; log) {
         writefln("\t%s", str);
      }

      if (log.length > 0) {
         writeln();
      }
   }

   return;
}

uint check_database() {
   shared uint rc = 0;
   uint function(uint)[] func_array;
   func_array ~= &check_abstracted;
   func_array ~= &check_award;
   func_array ~= &check_classol;
   func_array ~= &check_composer;
   func_array ~= &check_problem;
   func_array ~= &check_problem_composer;
   func_array ~= &check_problem_source;
   func_array ~= &check_source;
   func_array ~= &check_versions;
   func_array ~= &check_afters;
   func_array ~= &check_magranges;
   func_array ~= &check_nots;
   func_array ~= &check_cants;
   func_array ~= &check_cabs;
   func_array ~= &check_sants;
   func_array ~= &check_sabs;
   func_array ~= &check_vnots;
   func_array ~= &check_todoquote;

   foreach (uint i, func; parallel(func_array)) {
      uint r = func(i);
      core.atomic.atomicOp!"+="(rc, r);
   }

   return rc;
}

module check;

import std.parallelism;

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
import nots;
import cants;
import sants;
import vnots;
import logger;
import constant;

uint check_database() {
   logger log = new logger();
   uint rc = 0;
   uint function(uint, logger)[] func_array;
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
   func_array ~= &check_sants;
   func_array ~= &check_vnots;

   foreach (uint i, func; parallel(func_array)) {
      uint r = func(i, log);
   }

   return rc;
}

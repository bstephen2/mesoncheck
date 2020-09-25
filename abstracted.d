module abstracted;

import message;
import logger;
import constant;

uint check_abstracted(uint id, logger log) {
   uint rc;
   message mess;

   mess = new message(id, 1, "Checking table Abstracted ...");
   log.add_message(mess);
   mess = new message(id, 0, null);
   log.add_message(mess);

   return rc;
}

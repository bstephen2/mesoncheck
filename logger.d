module logger;

import message;
import constant;

class logger {
private:
   message[] messages;

public:
   this() {
      return;
   }

   void add_message(message mess) {
   
   	messages ~= mess;
   	
      return;
   }

private:

   void display_messages() {
      return;
   }
}

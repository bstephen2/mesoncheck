module message;
import constant;

class message {

public:
   uint id;
   uint level; //(0 means eof)
   string message;
   uint thread_id;

   this(uint in_id, uint in_level, string in_message, uint in_thread_id) {
      id = in_id;
      level = in_level;
      message = in_message;
      thread_id = in_thread_id;

      return;
   }
}

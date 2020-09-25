module message;
import constant;

class message {

public:
   uint id;
   uint level; //(0 means eof)
   string message;
 
   this(uint in_id, uint in_level, string in_message) {
      id = in_id;
      level = in_level;
      message = in_message;

      return;
   }
}

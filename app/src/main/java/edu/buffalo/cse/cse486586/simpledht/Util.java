package edu.buffalo.cse.cse486586.simpledht;

public class Util {

    public static final String JOIN_CHORD_SEPARATOR = ":::::";
    public static final String JOIN_CHORD_ACK = "JOIN_CHORD_ACK::::::";


    public static enum MessageType {
        MESSAGE_TYPE_JOIN_CHORD,
        MESSAGE_TYPE_JOIN_CHORD_ACK,
        MESSAGE_TYPE_UNKNOWN
    }

    public static MessageType getMessageType(String message) {
        if (message.contains(JOIN_CHORD_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD;
        } else if (message.equals(JOIN_CHORD_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_ACK;
        } else {
            //TODO: have failure case.
            return MessageType.MESSAGE_TYPE_UNKNOWN;
        }
    }

    public static String createJoinChordMessage(String node) {
        return "JOIN" + JOIN_CHORD_SEPARATOR + node;
    }

}

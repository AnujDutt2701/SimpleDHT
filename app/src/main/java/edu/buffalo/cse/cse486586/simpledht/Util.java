package edu.buffalo.cse.cse486586.simpledht;

public class Util {

    public static final String JOIN_CHORD_SEPARATOR = ":::::";
    public static final String JOIN_CHORD_ACK = "JOIN_CHORD_ACK::::::";
    public static final String JOIN_CHORD_SUCCESS_SEPARATOR = ";;;;;;";
    public static final String JOIN_CHORD_SUCCESS_ACK = "JOIN_CHORD_SUCCESS_ACK";
    public static final String PREDECESSOR_UPDATE_SEPARATOR = "======";
    public static final String PREDECESSOR_UPDATE_ACK = "PREDECESSOR_UPDATE_ACK";


    public static enum MessageType {
        MESSAGE_TYPE_JOIN_CHORD,
        MESSAGE_TYPE_JOIN_CHORD_ACK,
        MESSAGE_TYPE_JOIN_CHORD_SUCCESS,
        MESSAGE_TYPE_JOIN_CHORD_SUCCESS_ACK,
        MESSAGE_TYPE_PREDECESSOR_UPDATE,
        MESSAGE_TYPE_PREDECESSOR_UPDATE_ACK,
        MESSAGE_TYPE_UNKNOWN
    }

    public static MessageType getMessageType(String message) {
        if (message.contains(JOIN_CHORD_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD;
        } else if (message.equals(JOIN_CHORD_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_ACK;
        } else if (message.contains(JOIN_CHORD_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS;
        } else if (message.equals(JOIN_CHORD_SUCCESS_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS_ACK;
        } else if (message.contains(PREDECESSOR_UPDATE_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE;
        } else if (message.equals(PREDECESSOR_UPDATE_ACK)) {
            return MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE_ACK;
        } else {
            //TODO: have failure case.
            return MessageType.MESSAGE_TYPE_UNKNOWN;
        }
    }

    public static String createJoinChordMessage(String node) {
        return "JOIN" + JOIN_CHORD_SEPARATOR + node;
    }

    public static String createJoinChordSuccessMessage(String predecessorPort, String successorPort) {
        return predecessorPort + JOIN_CHORD_SUCCESS_SEPARATOR + successorPort;
    }

    public static String createPredecessorUpdateMessage(String requestingPort) {
        return "UPDATE" + PREDECESSOR_UPDATE_SEPARATOR + requestingPort;
    }

}

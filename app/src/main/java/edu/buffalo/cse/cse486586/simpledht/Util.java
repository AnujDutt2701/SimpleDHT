package edu.buffalo.cse.cse486586.simpledht;

public class Util {

    public static final String JOIN_CHORD_SEPARATOR = ":::::";
    public static final String JOIN_CHORD_ACK = "JOIN_CHORD_ACK::::::";
    public static final String JOIN_CHORD_SUCCESS_SEPARATOR = ";;;;;;";
    public static final String JOIN_CHORD_SUCCESS_ACK = "JOIN_CHORD_SUCCESS_ACK";
    public static final String PREDECESSOR_UPDATE_SEPARATOR = "======";
    public static final String PREDECESSOR_UPDATE_ACK = "PREDECESSOR_UPDATE_ACK";
    public static final String INSERT_VALUE_REQUEST_SEPARATOR = "------";
    public static final String INSERT_VALUE_REQUEST_ACK = "INSERT_VALUE_REQUEST_ACK";
    public static final String SINGLE_QUERY_SEPARATOR = "######";
    public static final String QUERY_RESPONSE_KEY_VALUE_SEPARATOR = ",,,,,,";
    public static final String QUERY_RESPONSE_SEPARATOR = "//////";
    public static final String QUERY_RESPONSE_EMPTY = "QUERY_RESPONSE_EMPTY";
    public static final String TOTAL_GLOBAL_QUERY_SEPARATOR = "&&&&&&";
    public static final String SINGLE_DELETE_SEPARATOR = "@@@@@@";
    public static final String TOTAL_GLOBAL_DELETE_SEPARATOR = ",,,,,,";
    public static final String DELETE_SUCCESS = "DELETE_SUCCESS";


    public static enum MessageType {
        MESSAGE_TYPE_JOIN_CHORD,
        MESSAGE_TYPE_JOIN_CHORD_ACK,
        MESSAGE_TYPE_JOIN_CHORD_SUCCESS,
        MESSAGE_TYPE_JOIN_CHORD_SUCCESS_ACK,
        MESSAGE_TYPE_PREDECESSOR_UPDATE,
        MESSAGE_TYPE_PREDECESSOR_UPDATE_ACK,
        MESSAGE_TYPE_INSERT_VALUE,
        MESSAGE_TYPE_INSERT_VALUE_ACK,
        MESSAGE_TYPE_SINGLE_QUERY,
        MESSAGE_TYPE_QUERY_RESPONSE,
        MESSAGE_TYPE_TOTAL_GLOBAL_QUERY,
        MESSAGE_TYPE_QUERY_RESPONSE_EMPTY,
        MESSAGE_TYPE_SINGLE_DELETE,
        MESSAGE_TYPE_TOTAL_GLOBAL_DELETE,
        MESSAGE_TYPE_DELETE_SUCCESS,
        MESSAGE_TYPE_UNKNOWN
    }

    public static MessageType getMessageType(String message) {
        if (message == null) {
            return MessageType.MESSAGE_TYPE_UNKNOWN;
        } else if (message.contains(JOIN_CHORD_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD;
        } else if (message.equals(JOIN_CHORD_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_ACK;
        } else if (message.contains(JOIN_CHORD_SUCCESS_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS;
        } else if (message.equals(JOIN_CHORD_SUCCESS_ACK)) {
            return MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS_ACK;
        } else if (message.contains(PREDECESSOR_UPDATE_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE;
        } else if (message.equals(PREDECESSOR_UPDATE_ACK)) {
            return MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE_ACK;
        } else if (message.contains(INSERT_VALUE_REQUEST_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_INSERT_VALUE;
        } else if (message.equals(INSERT_VALUE_REQUEST_ACK)) {
            return MessageType.MESSAGE_TYPE_INSERT_VALUE_ACK;
        } else if (message.contains(SINGLE_QUERY_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_SINGLE_QUERY;
        } else if (message.contains(QUERY_RESPONSE_KEY_VALUE_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_QUERY_RESPONSE;
        } else if (message.equals(QUERY_RESPONSE_EMPTY)) {
            return MessageType.MESSAGE_TYPE_QUERY_RESPONSE_EMPTY;
        } else if (message.contains(TOTAL_GLOBAL_QUERY_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_TOTAL_GLOBAL_QUERY;
        } else if (message.contains(SINGLE_DELETE_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_SINGLE_DELETE;
        } else if (message.contains(TOTAL_GLOBAL_DELETE_SEPARATOR)) {
            return MessageType.MESSAGE_TYPE_TOTAL_GLOBAL_DELETE;
        } else if (message.equals(DELETE_SUCCESS)) {
            return MessageType.MESSAGE_TYPE_DELETE_SUCCESS;
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

    public static String createInsertValueMessage(String key, String value) {
        return key + INSERT_VALUE_REQUEST_SEPARATOR + value;
    }

    public static String createSingleQueryMessage(String key) {
        return "SINGLE_QUERY" + SINGLE_QUERY_SEPARATOR + key;
    }

    public static String createTotalGlobalQueryMessage(String port) {
        return "TOTAL_GLOBAL_QUERY" + TOTAL_GLOBAL_QUERY_SEPARATOR + port;
    }

    public static String createSingleDeleteMessage(String key) {
        return "SINGLE_DELETE" + SINGLE_DELETE_SEPARATOR + key;
    }

    public static String createTotalGlobalDeleteMessage(String port) {
        return "TOTAL_GLOBAL_DELETE" + TOTAL_GLOBAL_DELETE_SEPARATOR + port;
    }

}

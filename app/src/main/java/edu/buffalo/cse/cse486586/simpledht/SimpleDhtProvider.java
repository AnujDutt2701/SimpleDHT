package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    public static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public static final String[] REMOTE_PORTS = new String[]{"11108", "11112", "11116", "11120", "11124"};
    public static final String MASTER_NODE = "5554";
    public static final int SERVER_PORT = 10000;
    public static String CURRENT_NODE;
    public static String CURRENT_NODE_HASH;

    public static String PREDECESSOR_NODE;
    public static String PREDECESSOR_NODE_HASH;
    public static String SUCCESSOR_NODE;
    public static String SUCCESSOR_NODE_HASH;

    static Uri mUri;
    SimpleDhtDbHelper mDbHelper;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("@")) {
            Log.d(TAG, "All the items stored on local device are to be deleted.");
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            String alteredProjection[] = {
                    "key_string AS key",
                    "value_string AS value"

            };

            db.delete(
                    SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                    null,              // The columns for the WHERE clause
                    null          // The values for the WHERE clause
            );
        } else if (selection.equals("*")) {
            Log.d(TAG, "All the items stored on entire DHT are to be deleted.");
            if ((PREDECESSOR_NODE == null && SUCCESSOR_NODE == null || (CURRENT_NODE.equals(PREDECESSOR_NODE)) && CURRENT_NODE.equals(SUCCESSOR_NODE))) {
                Log.d(TAG, "This process hasn't joined the chord ring");
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredProjection[] = {
                        "key_string AS key",
                        "value_string AS value"

                };

                db.delete(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        null,              // The columns for the WHERE clause
                        null          // The values for the WHERE clause
                );
            } else {
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredProjection[] = {
                        "key_string AS key",
                        "value_string AS value"

                };

                db.delete(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        null,              // The columns for the WHERE clause
                        null          // The values for the WHERE clause
                );

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(Util.createTotalGlobalDeleteMessage(CURRENT_NODE));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (Util.getMessageType(response = bufferedReader.readLine()) != Util.MessageType.MESSAGE_TYPE_DELETE_SUCCESS) {
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                    }
                    Log.d(TAG, "Received success from SUCCESSOR_NODE. " + response);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else if (Util.getMessageType(selection).equals(Util.MessageType.MESSAGE_TYPE_TOTAL_GLOBAL_DELETE)) {
            Log.d(TAG, "GDump is being propagated.");
            String startingPort = selection.split(Util.TOTAL_GLOBAL_DELETE_SEPARATOR)[1];
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            String alteredProjection[] = {
                    "key_string AS key",
                    "value_string AS value"

            };

            db.delete(
                    SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                    null,              // The columns for the WHERE clause
                    null          // The values for the WHERE clause
            );
            if (startingPort.equals(SUCCESSOR_NODE)) {
                // Do nothing.
            } else {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(Util.createTotalGlobalDeleteMessage(CURRENT_NODE));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (Util.getMessageType(response = bufferedReader.readLine()) != Util.MessageType.MESSAGE_TYPE_DELETE_SUCCESS) {
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                    }
                    Log.d(TAG, "Received success from SUCCESSOR_NODE. " + response);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            Log.d(TAG, "Specific item stored is to be deleted.");
            if (doesMessageExistOnDevice(genHash(selection))) {
                Log.d(TAG, "Specific item is stored in this process.");
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredSelectionArgs[] = {
                        selection
                };
                String alteredSelection = "key_string = ?";
                db.delete(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        alteredSelection,              // The columns for the WHERE clause
                        alteredSelectionArgs          // The values for the WHERE clause
                );
            } else {
                Log.d(TAG, "Specific item is not stored in this process.");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(Util.createSingleDeleteMessage(selection));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (Util.getMessageType(response = bufferedReader.readLine()) != Util.MessageType.MESSAGE_TYPE_DELETE_SUCCESS) {
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                    }
                    Log.d(TAG, "Received success from SUCCESSOR_NODE. " + response);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v(TAG, "Insert: " + values.toString());
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        String key = String.valueOf(values.get("key"));
        String value = String.valueOf(values.get("value"));

        if (shouldMessageBeAdded(genHash(key))) {
            Log.d(TAG, "Message should be added in this device.");
            db.replace(SimpleDhtContract.KeyValueEntry.TABLE_NAME, null, mDbHelper.formatToSqlContentValues(values));
        } else {
            Log.d(TAG, "Message should be forwarded.");
            forwardInsertValueRequestToSuccessor(key, value);
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        mDbHelper = new SimpleDhtDbHelper(getContext());
        String uriScheme = "content";
        String uriAuthority = "edu.buffalo.cse.cse486586.groupmessenger2.provider";
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(uriAuthority);
        uriBuilder.scheme(uriScheme);
        mUri = uriBuilder.build();
        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        CURRENT_NODE = portStr;
        Log.d(TAG, "Current node is: " + CURRENT_NODE);

        CURRENT_NODE_HASH = genHash(CURRENT_NODE);
        if (CURRENT_NODE.equals(MASTER_NODE)) {
            PREDECESSOR_NODE = CURRENT_NODE;
            PREDECESSOR_NODE_HASH = CURRENT_NODE_HASH;
            SUCCESSOR_NODE = CURRENT_NODE;
            SUCCESSOR_NODE_HASH = CURRENT_NODE_HASH;
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerAsyncTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        if (!CURRENT_NODE.equals(MASTER_NODE)) {
            joinChordRequest();
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d(TAG, "CURRENT_NODE: " + CURRENT_NODE + " PREDECESSOR_NODE: " + PREDECESSOR_NODE + " SUCCESSOR_NODE " + SUCCESSOR_NODE);
        Log.v("query", selection);
        if (selection.equals("*")) {
            Log.d(TAG, "All the items stored in the entire DHT are wanted.");
            if ((PREDECESSOR_NODE == null && SUCCESSOR_NODE == null || (CURRENT_NODE.equals(PREDECESSOR_NODE)) && CURRENT_NODE.equals(SUCCESSOR_NODE))) {
                Log.d(TAG, "This process hasn't joined the chord ring");
                String alteredSelectionArgs[] = {
                        selection
                };

                String alteredSelection = "key_string = ?";
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredProjection[] = {
                        "key_string AS key",
                        "value_string AS value"

                };
                return db.query(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        alteredProjection,// The array of columns to return (pass null to get all)
                        null,              // The columns for the WHERE clause
                        null,          // The values for the WHERE clause
                        null,                   // don't group the rows
                        null,                   // don't filter by row groups
                        null               // The sort order
                );
            } else {
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredProjection[] = {
                        "key_string AS key",
                        "value_string AS value"

                };

                Cursor cursor = db.query(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        alteredProjection,// The array of columns to return (pass null to get all)
                        null,              // The columns for the WHERE clause
                        null,          // The values for the WHERE clause
                        null,                   // don't group the rows
                        null,                   // don't filter by row groups
                        null               // The sort order
                );

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(Util.createTotalGlobalQueryMessage(CURRENT_NODE));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (true) {
                        response = bufferedReader.readLine();
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                        if (Util.getMessageType(response) == Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE ||
                                Util.getMessageType(response) == Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE_EMPTY) {
                            break;
                        }
                    }
                    Log.d(TAG, "Received response for query from SUCCESSOR_NODE. " + response);
                    String columns[] = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);
                    if (cursor != null && cursor.moveToFirst() && cursor.getCount() >= 1) {
                        int keyIndex = cursor.getColumnIndex("key");
                        int valueIndex = cursor.getColumnIndex("value");
                        do {
                            String returnKey = cursor.getString(keyIndex);
                            String returnValue = cursor.getString(valueIndex);
                            String rows[] = {returnKey, returnValue};
                            matrixCursor.addRow(rows);

                        } while (cursor.moveToNext());
                        cursor.close();
                    }
                    if (Util.getMessageType(response) != Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE_EMPTY) {
                        String[] keyValuePairs = response.split(Util.QUERY_RESPONSE_SEPARATOR);
                        for (String keyValue : keyValuePairs) {
                            String rows[] = {keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[0],
                                    keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[1]};
                            matrixCursor.addRow(rows);
                        }
                    }
                    return matrixCursor;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else if (Util.getMessageType(selection).equals(Util.MessageType.MESSAGE_TYPE_TOTAL_GLOBAL_QUERY)) {
            Log.d(TAG, "GDump is being propagated.");
            String startingPort = selection.split(Util.TOTAL_GLOBAL_QUERY_SEPARATOR)[1];
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            String alteredProjection[] = {
                    "key_string AS key",
                    "value_string AS value"

            };
            Cursor cursor = db.query(
                    SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                    alteredProjection,// The array of columns to return (pass null to get all)
                    null,              // The columns for the WHERE clause
                    null,          // The values for the WHERE clause
                    null,                   // don't group the rows
                    null,                   // don't filter by row groups
                    null               // The sort order
            );
            if (startingPort.equals(SUCCESSOR_NODE)) {
                return cursor;
            } else {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(selection);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (true) {
                        response = bufferedReader.readLine();
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                        if (Util.getMessageType(response) == Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE ||
                                Util.getMessageType(response) == Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE_EMPTY) {
                            break;
                        }
                    }
                    Log.d(TAG, "Received response for query from SUCCESSOR_NODE. " + response);
                    String columns[] = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);
                    if (cursor != null && cursor.moveToFirst() && cursor.getCount() >= 1) {
                        int keyIndex = cursor.getColumnIndex("key");
                        int valueIndex = cursor.getColumnIndex("value");
                        do {
                            String returnKey = cursor.getString(keyIndex);
                            String returnValue = cursor.getString(valueIndex);
                            String rows[] = {returnKey, returnValue};
                            matrixCursor.addRow(rows);

                        } while (cursor.moveToNext());
                        cursor.close();
                    }
                    if (Util.getMessageType(response) != Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE_EMPTY) {
                        String[] keyValuePairs = response.split(Util.QUERY_RESPONSE_SEPARATOR);
                        for (String keyValue : keyValuePairs) {
                            String rows[] = {keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[0],
                                    keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[1]};
                            matrixCursor.addRow(rows);
                        }
                    }
                    return matrixCursor;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else if (selection.equals("@")) {
            Log.d(TAG, "All the items stored in the current port are wanted.");
            Log.d(TAG, "CURRENT_NODE: " + CURRENT_NODE + "PREDECESSOR_NODE: " + PREDECESSOR_NODE + "SUCCESSOR_NODE: " + SUCCESSOR_NODE);
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            String alteredProjection[] = {
                    "key_string AS key",
                    "value_string AS value"

            };

            return db.query(
                    SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                    alteredProjection,// The array of columns to return (pass null to get all)
                    null,              // The columns for the WHERE clause
                    null,          // The values for the WHERE clause
                    null,                   // don't group the rows
                    null,                   // don't filter by row groups
                    null               // The sort order
            );
        } else {
            Log.d(TAG, "Specific item stored is wanted.");
            if (doesMessageExistOnDevice(genHash(selection))) {
                Log.d(TAG, "Specific item is stored in this process.");
                String alteredSelectionArgs[] = {
                        selection
                };

                String alteredSelection = "key_string = ?";
                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                String alteredProjection[] = {
                        "key_string AS key",
                        "value_string AS value"

                };
                return db.query(
                        SimpleDhtContract.KeyValueEntry.TABLE_NAME,   // The table to query
                        alteredProjection,// The array of columns to return (pass null to get all)
                        alteredSelection,              // The columns for the WHERE clause
                        alteredSelectionArgs,          // The values for the WHERE clause
                        null,                   // don't group the rows
                        null,                   // don't filter by row groups
                        null               // The sort order
                );
            } else {
                Log.d(TAG, "Specific item is not stored in this process.");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(SUCCESSOR_NODE) * 2);
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(Util.createSingleQueryMessage(selection));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String response;
                    while (Util.getMessageType(response = bufferedReader.readLine()) != Util.MessageType.MESSAGE_TYPE_QUERY_RESPONSE) {
                        Log.d(TAG, "Waiting for response from SUCCESSOR_NODE.");
                    }
                    Log.d(TAG, "Received response for query from SUCCESSOR_NODE. " + response);
                    String columns[] = {"key", "value"};
                    MatrixCursor matrixCursor = new MatrixCursor(columns);
                    String[] keyValuePairs = response.split(Util.QUERY_RESPONSE_SEPARATOR);
                    for (String keyValue : keyValuePairs) {
                        String rows[] = {keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[0],
                                keyValue.split(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR)[1]};
                        matrixCursor.addRow(rows);
                    }
                    return matrixCursor;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.d(TAG, "Returning null for query.");
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private static String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static void joinChordRequest() {
        new ClientAsyncTask(MASTER_NODE, Util.createJoinChordMessage(CURRENT_NODE), Util.MessageType.MESSAGE_TYPE_JOIN_CHORD).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    public static void forwardJoinPortRequest(String receivedMessage) {
        new ClientAsyncTask(SUCCESSOR_NODE, receivedMessage, Util.MessageType.MESSAGE_TYPE_JOIN_CHORD).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    public static void informRequestingPortOfSuccess(String requestingPort, String predecessorPort, String successorPort) {
        new ClientAsyncTask(requestingPort, Util.createJoinChordSuccessMessage(predecessorPort, successorPort), Util.MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    public static void informPriorPredecessorPortOfUpdate(String priorPredecessorPort, String requestingPort) {
        new ClientAsyncTask(priorPredecessorPort, Util.createPredecessorUpdateMessage(requestingPort), Util.MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    private void forwardInsertValueRequestToSuccessor(String key, String value) {
        new ClientAsyncTask(SUCCESSOR_NODE, Util.createInsertValueMessage(key, value), Util.MessageType.MESSAGE_TYPE_INSERT_VALUE).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    public static class ClientAsyncTask extends AsyncTask<Void, Void, Void> {

        String recipient;
        String message;
        Util.MessageType messageType;

        public ClientAsyncTask(String recipient, String message, Util.MessageType messageType) {
            this.recipient = String.valueOf(Integer.valueOf(recipient) * 2);
            this.message = message;
            this.messageType = messageType;
        }

        @Override
        protected Void doInBackground(Void... voids) {
            if (messageType.equals(Util.MessageType.MESSAGE_TYPE_JOIN_CHORD)) {
                Log.d(TAG, "Sending JOIN_CHORD to a port.");
                Socket socket;
                PrintWriter printWriter;

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(recipient));
                    printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(message);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String receivedMessage = bufferedReader.readLine();
                    if (receivedMessage != null) {
                        if (Util.getMessageType(receivedMessage) == Util.MessageType.MESSAGE_TYPE_JOIN_CHORD_ACK) {
                            bufferedReader.close();
                            printWriter.close();
                            socket.close();
                        }
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (messageType.equals(Util.MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS)) {
                Log.d(TAG, "Sending JOIN_CHORD_SUCCESS to requesting node for joining the DHT.");
                Socket socket;
                PrintWriter printWriter;

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(recipient));
                    printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(message);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String receivedMessage = bufferedReader.readLine();
                    if (Util.getMessageType(receivedMessage) == Util.MessageType.MESSAGE_TYPE_JOIN_CHORD_SUCCESS_ACK) {
                        bufferedReader.close();
                        printWriter.close();
                        socket.close();
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (messageType.equals(Util.MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE)) {
                Log.d(TAG, "Sending PREDECESSOR_UPDATE to prior predecessor node.");
                Socket socket;
                PrintWriter printWriter;

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(recipient));
                    printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(message);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String receivedMessage = bufferedReader.readLine();
                    if (Util.getMessageType(receivedMessage) == Util.MessageType.MESSAGE_TYPE_PREDECESSOR_UPDATE_ACK) {
                        bufferedReader.close();
                        printWriter.close();
                        socket.close();
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (messageType.equals(Util.MessageType.MESSAGE_TYPE_INSERT_VALUE)) {
                Log.d(TAG, "Sending INSERT_VALUE to successor node.");
                Socket socket;
                PrintWriter printWriter;

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(recipient));
                    printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(message);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String receivedMessage = bufferedReader.readLine();
                    if (Util.getMessageType(receivedMessage) == Util.MessageType.MESSAGE_TYPE_INSERT_VALUE_ACK) {
                        bufferedReader.close();
                        printWriter.close();
                        socket.close();
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    public class ServerAsyncTask extends AsyncTask<ServerSocket, Void, Void> {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            while (true) {
                Socket incomingSocket;
                BufferedReader bufferedReader;
                PrintWriter printWriter;
                try {
                    incomingSocket = serverSockets[0].accept();
                    bufferedReader = new BufferedReader(new InputStreamReader(incomingSocket.getInputStream()));
                    String receivedMessage = bufferedReader.readLine();
                    switch (Util.getMessageType(receivedMessage)) {
                        case MESSAGE_TYPE_JOIN_CHORD:
                            String requestingPort = receivedMessage.split(Util.JOIN_CHORD_SEPARATOR)[1];
                            String requestingPortHash = genHash(requestingPort);
                            boolean shouldNodeBeAdded = shouldNodeBeAddedAsPredecessor(requestingPortHash);
                            Log.d(TAG, "MESSAGE_TYPE_JOIN_CHORD: " + requestingPort + ". Should be added: " + shouldNodeBeAdded);
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.JOIN_CHORD_ACK);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            if (shouldNodeBeAdded) {
                                String priorPredecessorPort = PREDECESSOR_NODE;
                                String priorPredecessorPortHash = PREDECESSOR_NODE_HASH;
                                String priorSuccessorPort = SUCCESSOR_NODE;
                                String priorSuccessorPortHash = SUCCESSOR_NODE_HASH;
                                processIncomingJoinChordMessage(requestingPort, requestingPortHash);
                                informRequestingPortOfSuccess(requestingPort, priorPredecessorPort, CURRENT_NODE);
                                if (priorSuccessorPort.equals(SUCCESSOR_NODE)) {
                                    informPriorPredecessorPortOfUpdate(priorPredecessorPort, requestingPort);
                                }

                            } else {
                                forwardJoinPortRequest(receivedMessage);
                            }
                            break;
                        case MESSAGE_TYPE_JOIN_CHORD_SUCCESS:
                            PREDECESSOR_NODE = receivedMessage.split(Util.JOIN_CHORD_SUCCESS_SEPARATOR)[0];
                            PREDECESSOR_NODE_HASH = genHash(PREDECESSOR_NODE);
                            SUCCESSOR_NODE = receivedMessage.split(Util.JOIN_CHORD_SUCCESS_SEPARATOR)[1];
                            SUCCESSOR_NODE_HASH = genHash(SUCCESSOR_NODE);
                            Log.d(TAG, "MESSAGE_TYPE_JOIN_CHORD_SUCCESS: " + " PREDECESSOR_NODE: " + PREDECESSOR_NODE + " SUCCESSOR_NODE:" + SUCCESSOR_NODE);
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.JOIN_CHORD_SUCCESS_ACK);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            break;
                        case MESSAGE_TYPE_PREDECESSOR_UPDATE:
                            SUCCESSOR_NODE = receivedMessage.split(Util.PREDECESSOR_UPDATE_SEPARATOR)[1];
                            SUCCESSOR_NODE_HASH = genHash(SUCCESSOR_NODE);
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.PREDECESSOR_UPDATE_ACK);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            break;
                        case MESSAGE_TYPE_INSERT_VALUE:
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.INSERT_VALUE_REQUEST_ACK);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            ContentValues contentValues = new ContentValues();
                            contentValues.put("key", receivedMessage.split(Util.INSERT_VALUE_REQUEST_SEPARATOR)[0]);
                            contentValues.put("value", receivedMessage.split(Util.INSERT_VALUE_REQUEST_SEPARATOR)[1]);
                            insert(mUri, contentValues);
                            break;
                        case MESSAGE_TYPE_SINGLE_QUERY:
                            Log.d(TAG, "MESSAGE_TYPE_SINGLE_QUERY" + receivedMessage);
                            Cursor singleQuerycursor = query(mUri, null, receivedMessage.split(Util.SINGLE_QUERY_SEPARATOR)[1], null, null);
                            StringBuilder singleQueryResponse = new StringBuilder();
                            boolean first = true;
                            if (singleQuerycursor != null && singleQuerycursor.moveToFirst() && singleQuerycursor.getCount() >= 1) {
                                do {
                                    int singleQueryKeyIndex = singleQuerycursor.getColumnIndex("key");
                                    int singleQueryValueIndex = singleQuerycursor.getColumnIndex("value");
                                    String returnKey = singleQuerycursor.getString(singleQueryKeyIndex);
                                    String returnValue = singleQuerycursor.getString(singleQueryValueIndex);
                                    if (first) {
                                        singleQueryResponse.append(returnKey).append(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR).append(returnValue);
                                        first = false;
                                    } else {
                                        singleQueryResponse.append(Util.QUERY_RESPONSE_SEPARATOR).append(returnKey).append(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR).append(returnValue);
                                    }

                                } while (singleQuerycursor.moveToNext());
                            }
                            Log.d(TAG, "Query satisfied: " + singleQueryResponse.toString());
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(singleQueryResponse.toString());
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            if (singleQuerycursor != null) {
                                singleQuerycursor.close();
                            }
                            break;
                        case MESSAGE_TYPE_TOTAL_GLOBAL_QUERY:
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            Cursor globalQueryCursor = query(mUri, null, receivedMessage, null, null);
                            if (globalQueryCursor != null && globalQueryCursor.moveToFirst() && globalQueryCursor.getCount() >= 1) {
                                StringBuilder globalQueryResponse = new StringBuilder();
                                int globalQueryKeyIndex = globalQueryCursor.getColumnIndex("key");
                                int globalQueryValueIndex = globalQueryCursor.getColumnIndex("value");

                                boolean first1 = true;
                                do {
                                    String returnKey = globalQueryCursor.getString(globalQueryKeyIndex);
                                    String returnValue = globalQueryCursor.getString(globalQueryValueIndex);
                                    if (first1) {
                                        globalQueryResponse.append(returnKey).append(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR).append(returnValue);
                                        first1 = false;
                                    } else {
                                        globalQueryResponse.append(Util.QUERY_RESPONSE_SEPARATOR).append(returnKey).append(Util.QUERY_RESPONSE_KEY_VALUE_SEPARATOR).append(returnValue);
                                    }

                                } while (globalQueryCursor.moveToNext());
                                printWriter.println(globalQueryResponse.toString());
                            } else {
                                printWriter.println(Util.QUERY_RESPONSE_EMPTY);
                            }
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            if (globalQueryCursor != null) {
                                globalQueryCursor.close();
                            }
                            break;
                        case MESSAGE_TYPE_SINGLE_DELETE:
                            Log.d(TAG, "MESSAGE_TYPE_SINGLE_DELETE" + receivedMessage);
                            delete(mUri, receivedMessage.split(Util.SINGLE_DELETE_SEPARATOR)[1], null);
                            Log.d(TAG, "Delete satisfied.");
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.DELETE_SUCCESS);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            break;
                        case MESSAGE_TYPE_TOTAL_GLOBAL_DELETE:
                            Log.d(TAG, "MESSAGE_TYPE_TOTAL_GLOBAL_DELETE" + receivedMessage);
                            delete(mUri, receivedMessage, null);
                            printWriter = new PrintWriter(incomingSocket.getOutputStream(), true);
                            printWriter.println(Util.DELETE_SUCCESS);
                            printWriter.close();
                            bufferedReader.close();
                            incomingSocket.close();
                            break;
                    }
                } catch (IOException e) {

                }
            }
        }
    }

    public static void processIncomingJoinChordMessage(String requestingPort, String requestingPortHash) {
        if (PREDECESSOR_NODE.equals(CURRENT_NODE) && SUCCESSOR_NODE.equals(CURRENT_NODE)) {
            // First node is requesting.
            PREDECESSOR_NODE = requestingPort;
            PREDECESSOR_NODE_HASH = requestingPortHash;
            SUCCESSOR_NODE = requestingPort;
            SUCCESSOR_NODE_HASH = requestingPortHash;
        } else {
            PREDECESSOR_NODE = requestingPort;
            PREDECESSOR_NODE_HASH = requestingPortHash;
        }
    }

    public static boolean shouldNodeBeAddedAsPredecessor(String requestingPortHash) {
        boolean result = false;
        if (requestingPortHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && requestingPortHash.compareTo(CURRENT_NODE_HASH) < 0) {
            // This requesting port must be added as predecessor.
            result = true;
            Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT1: " + result);
            return result;
        } else if (requestingPortHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && requestingPortHash.compareTo(CURRENT_NODE_HASH) > 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                result = true;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT2: " + result);
                return result;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                result = true;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT3: " + result);
                return result;
            } else {
                result = false;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT4: " + result);
                return result;
            }
        } else if (requestingPortHash.compareTo(PREDECESSOR_NODE_HASH) < 0 && requestingPortHash.compareTo(CURRENT_NODE_HASH) < 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                result = true;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT5: " + result);
                return result;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                result = true;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT6: " + result);
                return result;
            } else {
                result = false;
                Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT7: " + result);
                return result;
            }
        } else {
            result = false;
            Log.d(TAG, "shouldNodeBeAddedAsPredecessor: " + requestingPortHash + " CURRENT_NODE_HASH: " + CURRENT_NODE_HASH + " PREDECESSOR_NODE_HASH: " + PREDECESSOR_NODE_HASH + " SUCCESSOR_NODE_HASH: " + SUCCESSOR_NODE_HASH + " RESULT8: " + result);
            return result;
        }
    }

    public static boolean shouldMessageBeAdded(String keyHash) {
        Log.d(TAG, "shouldMessageBeAdded: " + keyHash);
        if (PREDECESSOR_NODE_HASH == null) {
            Log.d(TAG, "This node hasn't joined the chord.");
            return true;
        } else if (keyHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && keyHash.compareTo(CURRENT_NODE_HASH) < 0) {
            // This requesting port must be added as predecessor.
            return true;
        } else if (keyHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && keyHash.compareTo(CURRENT_NODE_HASH) > 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                return true;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                return true;
            } else {
                return false;
            }
        } else if (keyHash.compareTo(PREDECESSOR_NODE_HASH) < 0 && keyHash.compareTo(CURRENT_NODE_HASH) < 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                return true;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public static boolean doesMessageExistOnDevice(String messageHash) {
        Log.d(TAG, "doesMessageExistOnDevice: " + messageHash);
        if (PREDECESSOR_NODE_HASH == null) {
            Log.d(TAG, "This node hasn't joined the chord.");
            return true;
        } else if (messageHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && messageHash.compareTo(CURRENT_NODE_HASH) < 0) {
            // This requesting port must be added as predecessor.
            return true;
        } else if (messageHash.compareTo(PREDECESSOR_NODE_HASH) > 0 && messageHash.compareTo(CURRENT_NODE_HASH) > 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                return true;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                return true;
            } else {
                return false;
            }
        } else if (messageHash.compareTo(PREDECESSOR_NODE_HASH) < 0 && messageHash.compareTo(CURRENT_NODE_HASH) < 0) {
            if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) > 0) {
                return true;
            } else if (PREDECESSOR_NODE_HASH.compareTo(CURRENT_NODE_HASH) == 0) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}

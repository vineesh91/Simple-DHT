package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static String portStr;
    static final int SERVER_PORT = 10000;
    static String predecessor;
    static String successor;
    static Semaphore query_sema;
    String queryResult = "";
    String portToSend = null;
    String ID;
    private SQLiteDatabase db;
    private MainDatabaseHelper mOpenHelper;
    private static final String SQL_CREATE_MAIN = "CREATE TABLE IF NOT EXISTS " +
            "main " + "(" + " key TEXT PRIMARY KEY, " + " value TEXT )";
    private static final String DBNAME = "messages";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@") || selection.equals("*")) {
            SQLiteDatabase db = mOpenHelper.getWritableDatabase();
            mOpenHelper.onCreate(db);
            db.delete("main",null,null);
            if(selection.equals("*")) {
                String msg = "deleteall"+","+portStr;
                if(!successor.equals("")) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor, "delete");
                }
            }
        }
        else {
            try {
                String kID = genHash(selection);
                String myId = genHash(portStr);
                String prvId = genHash(predecessor);
                if (successor.isEmpty() || kID.compareTo(myId) == 0) {
                    db.delete("main","key=?",new String[]{selection});
                }
                else if(kID.compareTo(prvId) > 0 && kID.compareTo(myId) <= 0) {
                    db.delete("main","key=?",new String[]{selection});
                }
                else if(myId.compareTo(prvId) < 0 && ((kID.compareTo(myId) < 0) || (kID.compareTo(prvId) > 0))) {
                    db.delete("main","key=?",new String[]{selection});
                }
                else {
                    String msg = "delete"+","+selection + "," +portStr;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"delete");
                }
            }catch(NoSuchAlgorithmException e) {
                Log.e(TAG,"genhash failed");
            }

        }
        //c = qBuilder.buildQuery(.)
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {

            String kID = genHash(values.getAsString("key"));
            String prevId = genHash(predecessor);
            String sucId = genHash(successor);
            String myId = genHash(portStr);
            /*if(!successor.isEmpty() && kID.compareTo(genHash(maxPort)) > 0 && minPort.equals(portStr)) {
                db.insertWithOnConflict("main", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.e(TAG,"********Inserting within******"+values.getAsString("value"));
            }*/
            if (successor.isEmpty() || kID.compareTo(myId) == 0) {
                db.insertWithOnConflict("main", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.e(TAG,"********Inserting within******"+values.getAsString("value"));
            }
            else if(kID.compareTo(myId) > 0 && kID.compareTo(sucId) <= 0) {
                String msg = "insertinyou"+","+values.getAsString("key") + "," + values.getAsString("value")+","+portStr;
                //String msgRcvd = sendMsg(msg,successor);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"insert");
                Log.e(TAG,"#########Inserting successor("+successor+")########"+values.getAsString("value"));
            }
            else if(myId.compareTo(sucId) > 0 && ((kID.compareTo(myId) > 0) || (kID.compareTo(sucId) < 0))) {
                String msg = "insertinyou"+","+values.getAsString("key") + "," + values.getAsString("value")+","+portStr;
                //String msgRcvd = sendMsg(msg,successor);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"insert");
                Log.e(TAG,"#########Inserting successor("+successor+")########"+values.getAsString("value"));
            }
            else {
                String msg = "insert"+","+values.getAsString("key") + "," + values.getAsString("value")+","+portStr;
                //String msgRcvd = sendMsg(msg,successor);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"insert");
                Log.e(TAG,"#########Inserting successor("+successor+")########"+values.getAsString("value"));
            }
        } catch (Exception ex) {
            Log.v("insert", ex.getMessage());
        }
        Log.v("insert", values.toString());
        return uri;
        //return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //getting the emulator port number
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        predecessor = "";
        successor = "";
        query_sema = new Semaphore(0);
        //assigning the ID based on the emulator port number
        try {
            ID = genHash(portStr);
        } catch (NoSuchAlgorithmException exp) {
            Log.e(TAG, "No such Algorithm");
        }
        mOpenHelper = new MainDatabaseHelper(getContext(), DBNAME, null, 1);
        db = mOpenHelper.getWritableDatabase();
        mOpenHelper.onCreate(db);
        //server socket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "ServerTask failed IOException");
        }
        if(!portStr.equals("5554")) {
            nodeJoin();
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        //reference:-http://developer.android.com/
        selectionArgs = new String[]{selection};
        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables("main");
        Cursor c;
        try {

            if (selection.equals("*") || selection.equals("@")) {
                c = qBuilder.query(db,
                        projection,
                        null,
                        null,
                        null,
                        null,
                        sortOrder);
                if (selection.equals("*")) {
                    String msg = "queryall" + "," + portStr;
                    String rcvd = "";
                    if (!successor.equals("")) {
                        //rcvd = sendMsg(msg, successor);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor, "queryall");
                        query_sema.acquire();
                    }
                    MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                    while (c.moveToNext()) {
                        mat.addRow(new Object[]{c.getString(0), c.getString(1)});
                    }
                    rcvd = queryResult;
                    queryResult = "";
                    if (!rcvd.equals("")) {
                        String[] spltVal = rcvd.split("-");
                        for (String vals : spltVal) {
                            String[] keyVal = vals.split(":");
                            mat.addRow(new Object[]{keyVal[0], keyVal[1]});
                        }
                    }
                    mat.setNotificationUri(getContext().getContentResolver(), uri);
                    return mat;
                }
            } else {
                String kHash = "";
                String sucId = "";
                String prvId = "";
                String myHash = "";
                Log.e(TAG,"%%%%%%%New query : "+selection);
                try {
                    kHash = genHash(selection);
                    sucId = genHash(successor);
                    prvId = genHash(predecessor);
                    myHash = genHash(portStr);
                } catch (NoSuchAlgorithmException ex) {
                    Log.e(TAG, "No such algorithm");
                }
                if (successor.equals("") || kHash.compareTo(myHash) == 0 ||
                        (kHash.compareTo(prvId) > 0 && kHash.compareTo(myHash) < 0)) {
                    Log.e(TAG,"querying within");
                    c = qBuilder.query(db,
                            projection,
                            "key=?",
                            selectionArgs,
                            null,
                            null,
                            sortOrder);
                } else if ((kHash.compareTo(myHash) > 0 && kHash.compareTo(sucId) <= 0)
                        ||((sucId.compareTo(myHash) < 0) &&(kHash.compareTo(myHash) >0 || kHash.compareTo(sucId) < 0))) {
                    String msg = "queryfromyou"  + "," + selection + "," + portStr+","+"n";
                    Log.e(TAG,"querying from "+successor+" key"+selection);
                    //String rcvd = sendMsg(msg,successor);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor, "query");
                    query_sema.acquire();
                    String[] spltVal = queryResult.split(":");
                    Log.e(TAG,"++++++result obtained+++++++++");
                    queryResult = "";
                    MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                    mat.addRow(new Object[]{spltVal[0], spltVal[1]});
                    mat.setNotificationUri(getContext().getContentResolver(), uri);
                    return mat;
                } else {
                    String msg = "queryfwd" + "," + selection + "," + portStr ;
                    //String rcvd = sendMsg(msg,successor);
                    Log.e(TAG,"Forwarding the query request :"+selection);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor, "queryfwd");
                    query_sema.acquire();
                    String[] spltVal = queryResult.split(":");
                    Log.e(TAG,"++++++result obtained+++++++++");
                    queryResult = "";
                    MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                    mat.addRow(new Object[]{spltVal[0], spltVal[1]});
                    mat.setNotificationUri(getContext().getContentResolver(), uri);
                    return mat;
                }
            }
            c.setNotificationUri(getContext().getContentResolver(), uri);
            Log.v("query", selection);
        }catch(InterruptedException e) {
            Log.e(TAG,"Interrupted");
            return null;
        }
        return c;
        //return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {
        //reference:-http://developer.android.com/
        /*
         * Instantiates an open helper for the provider's SQLite data repository
         * Do not do database creation and upgrade here.
         */
        MainDatabaseHelper(Context context, String dbname, Cursor cursor, int versionNumber) {
            super(context, DBNAME, null, 1);
        }

        /*
         * Creates the data repository. This is called when the provider attempts to open the
         * repository and SQLite reports that it doesn't exist.
         */
        public void onCreate(SQLiteDatabase db) {
            // Creates the main table
            db.execSQL(SQL_CREATE_MAIN);
        }

        /*
         * Creates the data repository. This is called when the provider attempts to open the
         * repository and SQLite reports that it doesn't exist.
         */
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
    }
    private void sendMsg(String... msgs) {

     new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0], msgs[1]);

    }

    private class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msg = "";
            try {
                Log.e(TAG,"Client side sending message : "+msgs[0]);
                int port = Integer.parseInt(msgs[1]) * 2;
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Integer.toString(port)));
                String msgToSend = msgs[0];
                //Reference-docs.oracle.com
                PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                out1.println(msgToSend);
                msg = in.readLine();

                if(msgs[2].equals("joinsendtosuc")) {
                    SQLiteDatabase dbins = mOpenHelper.getWritableDatabase();
                    mOpenHelper.onCreate(dbins);
                    ContentValues toPut = new ContentValues();
                    String[] dat = msg.split(",");
                    for(String kvpair : dat) {
                        String[] datpair = kvpair.split(":");
                        if(datpair[0].equals("pred")) {
                            publishProgress(datpair[1],"pred");
                        }
                        else {
                            toPut.put("key", datpair[0]);
                            toPut.put("value", datpair[1]);
                            dbins.insertWithOnConflict("main", null, toPut, SQLiteDatabase.CONFLICT_REPLACE);
                        }
                    }

                }
                else if(msgs[2].equals("query") && !msg.equals("")) {
                    publishProgress(msg,"queryresult");
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            if(strings[1].equals("suc")) {
                if (strings[0] != null && !strings[0].equals("")) {
                    successor = strings[0];
                }
            }
            else if(strings[1].equals("pred")) {
                if (strings[0] != null && !strings[0].equals("")) {
                    predecessor = strings[0];
                }
            }
            else if(strings[1].equals("porttosend")) {
                if (strings[0] != null && !strings[0].equals("")) {
                    portToSend = strings[0];
                }
            }
            else if(strings[1].equals("queryresult")) {
                queryResult = strings[0];
                Log.e(TAG,"Query result is "+queryResult);
                query_sema.release();
            }
        }
    }

    private void nodeJoin() {

            String msgToSend = "newnode"+","+portStr;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, "5554","join");
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                //Reference-docs.oracle.com
                while (true) {
                    Socket socket = serverSocket.accept();
                    String rcvd = "";
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    String inpMsg = in.readLine();
                    String[] sepMsg = inpMsg.split(",");
                    SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                    mOpenHelper.onCreate(db);

                    String sucId = genHash(successor);
                    String myId = genHash(portStr);
                    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                    if(sepMsg[0].equals("insert")) {
                        String kHash = genHash(sepMsg[1]);

                            ContentValues toPut = new ContentValues();
                            toPut.put("key", sepMsg[1]);
                            toPut.put("value", sepMsg[2]);
                            out.println("");
                            if(kHash.compareTo(myId) > 0 && kHash.compareTo(sucId) <= 0) {
                                String msg = "insertinyou"+","+toPut.getAsString("key") + "," + toPut.getAsString("value")+","+portStr;
                                //String msgRcvd = sendMsg(msg,successor);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"insert");
                                Log.e(TAG,"#########Inserting successor("+successor+")########"+toPut.getAsString("value"));
                            }
                            else if(myId.compareTo(sucId) > 0 && ((kHash.compareTo(myId) > 0) || (kHash.compareTo(sucId) < 0))) {
                                String msg = "insertinyou"+","+toPut.getAsString("key") + "," + toPut.getAsString("value")+","+portStr;
                                //String msgRcvd = sendMsg(msg,successor);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"insert");
                                Log.e(TAG,"#########Inserting successor("+successor+")########"+toPut.getAsString("value"));
                            }
                            else {
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, inpMsg, successor,"insert");
                                Log.e(TAG,"#########Inserting successor("+successor+")########"+toPut.getAsString("value"));
                            }

                        //insert(uri, toPut);

                    }
                    else if(sepMsg[0].equals("insertinyou")) {
                        //SQLiteDatabase db = mOpenHelper.getWritableDatabase();
                        mOpenHelper.onCreate(db);
                        ContentValues toPut = new ContentValues();
                        toPut.put("key", sepMsg[1]);
                        toPut.put("value", sepMsg[2]);
                        out.println("");
                        db.insertWithOnConflict("main", null, toPut, SQLiteDatabase.CONFLICT_REPLACE);
                        Log.e(TAG,"********Inserting within******"+toPut.getAsString("value"));
                    }
                    else if(sepMsg[0].equals("queryall")) {
                            if(sepMsg[1].equals(portStr)) {
                                out.println("");
                                //query_sema.release();
                            }
                            else {
                                out.println("");
                                SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                                qBuilder.setTables("main");
                                Cursor resultCursor = qBuilder.query(db,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null);
                                rcvd = "";
                                while (resultCursor.moveToNext()) {
                                    rcvd = rcvd + resultCursor.getString(0) + ":" + resultCursor.getString(1);
                                    if(!resultCursor.isLast()) {
                                        rcvd = rcvd + "-";
                                    }
                                }
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryresultall" + "," + portStr + "," + rcvd, sepMsg[1], "queryall");


                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryall" + "," + sepMsg[1], successor, "queryall");
                            }
                    }
                    else if(sepMsg[0].equals("queryresultall")) {
                        out.println("");
                        if(queryResult.equals("")) {
                            if(sepMsg.length > 2) {
                                queryResult = sepMsg[2];
                            }
                        }
                        else {
                            if(sepMsg.length > 2) {
                                queryResult = queryResult + "-" + sepMsg[2];
                            }
                        }
                        Log.e(TAG,"In query result all with sending port "+sepMsg[1]+" and my pred "+predecessor);
                        if(sepMsg[1].equals(predecessor)) {
                            query_sema.release();
                        }
                    }
                    else if(sepMsg[0].equals("queryfromyou")) {
                        String[] selectionArgs = new String[]{sepMsg[1]};
                        //SQLiteDatabase db = mOpenHelper.getReadableDatabase();
                        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                        qBuilder.setTables("main");
                        Cursor resultCursor = qBuilder.query(db,
                                null,
                                "key=?",
                                selectionArgs,
                                null,
                                null,
                                null);

                        rcvd ="";
                        while(resultCursor.moveToNext()) {
                             rcvd = rcvd + resultCursor.getString(0)+":"+resultCursor.getString(1);
                        }
                        resultCursor.close();
                        Log.e(TAG,"&&&&The result from "+portStr+" is "+rcvd+ "not sending to pred? "+sepMsg[3]);
                        if(sepMsg[3].equals("n")) {
                            Log.e(TAG,"Sending back");
                            out.println(rcvd);
                        }
                        else {
                            out.println("");
                            Log.e(TAG,"Sending to "+sepMsg[2]);
                            rcvd = "queryresult"+","+rcvd;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, rcvd, sepMsg[2],"queryfwd");
                        }
                    }
                    else if(sepMsg[0].equals("queryresult")) {
                        out.println("");
                        queryResult = sepMsg[1];
                        Log.e(TAG,"Query result obtained : "+queryResult+" and Q len: "+query_sema.getQueueLength());

                        query_sema.release();
                    }
                    else if(sepMsg[0].equals("queryfwd")) {
                        out.println("");
                        String kHash = genHash(sepMsg[1]);
                        String msgToSend;
                        if ((kHash.compareTo(myId) > 0 && kHash.compareTo(sucId) <= 0)
                                ||((sucId.compareTo(myId) < 0) &&(kHash.compareTo(myId) >0 || kHash.compareTo(sucId) < 0))){
                            msgToSend = "queryfromyou"+","+sepMsg[1]+","+sepMsg[2]+","+"y";
                            Log.e(TAG,"querying from "+successor+" key"+ sepMsg[1] + " to send to "+sepMsg[2]);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor,"searchquery");
                        }
                        else {
                            msgToSend = "queryfwd" + "," + sepMsg[1] + "," + sepMsg[2] ;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor,"queryfwd");
                        }

                    }
                    else if(sepMsg[0].equals("query")) {
                        out.println(rcvd);
                        Cursor resultCursor = query(uri,null,sepMsg[1],null,null);
                        if(sepMsg[1].equals("@")) {
                            if(!sepMsg[2].equals(successor)) {
                                String msg = "query"+","+portStr+","+"@";
                                //rcvd = sendMsg(msg,successor);
                            }
                        }
                        while(resultCursor.moveToNext()) {
                            if(!rcvd.equals("")) {
                                rcvd = rcvd +","+resultCursor.getString(0)+":"+resultCursor.getString(1);
                            }
                        }

                    }
                    else if(sepMsg[0].equals("yoursuccessor")) {
                        String msgToSend = "joinrequesttosuc"+","+portStr;
                        successor = sepMsg[1];
                        out.println("");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, sepMsg[1],"joinsendtosuc");

                    }
                    else if(sepMsg[0].equals("joinrequesttosuc")) {
                        //Transferring data
                        try {
                            String prvId = genHash(sepMsg[1]);
                            SQLiteDatabase dbtrnsfr = mOpenHelper.getWritableDatabase();
                            mOpenHelper.onCreate(dbtrnsfr);
                            SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                            qBuilder.setTables("main");
                            Cursor resultCursor = qBuilder.query(db,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null);
                            HashMap<String, String> hmap = new HashMap<String, String>();
                            while (resultCursor.moveToNext()) {
                                hmap.put(resultCursor.getString(0), resultCursor.getString(1));
                            }
                            String msgToSend ="";
                            int dcnt = 0;
                            for(String kVal : hmap.keySet()) {
                                String keyId = genHash(kVal);

                                if(keyId.compareTo(prvId) <= 0) {
                                    dcnt = dbtrnsfr.delete("main","key=?",new String[]{kVal});
                                }
                                msgToSend = msgToSend+kVal + ":" +hmap.get(kVal) + ",";
                            }
                            if (predecessor.equals("")) {
                                msgToSend = msgToSend + "pred"+":"+portStr;
                                out.println(msgToSend);
                            } else {
                                msgToSend = msgToSend + "pred"+":"+predecessor;
                                out.println(msgToSend);
                            }
                            predecessor = sepMsg[1];
                        }catch(NoSuchAlgorithmException e) {
                            Log.e(TAG," Hashing failed");
                        }
                    }
                    else if(sepMsg[0].equals("newnodeFwd")) {

                        String portHash = genHash(sepMsg[1]);
                        portToSend =null;
                        String myHash = genHash(portStr);
                        String sucHash = genHash(successor);

                            if(successor.equals("5554")) {
                                //String portHash = genHash(sepMsg[1]);
                                String msgToSend = "yoursuccessor" + "," + "5554";
                                successor = sepMsg[1];
                                out.println("");
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, sepMsg[1], "newnode");
                            }
                            else if(sucHash.compareTo(portHash) > 0 && portHash.compareTo(myHash) > 0) {
                                portToSend = successor;
                                String msgToSend = "yoursuccessor"+","+successor;
                                successor = sepMsg[1];
                                //updateMaxMin(portHash,sepMsg[1]);
                                out.println("");
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, sepMsg[1],"newnode");
                            }
                            else {
                                String msgToSend = "newnodeFwd"+","+sepMsg[1];
                                out.println("");
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor,"newnode");
                            }

                    }
                    else if(sepMsg[0].equals("newnode")) {
                        try {
                            String portHash = genHash(sepMsg[1]);
                            portToSend =null;
                            String myHash = genHash(portStr);
                            String sucHash = genHash(successor);
                            if(successor.equals("")) {
                                portToSend = portStr;
                                //predecessor = sepMsg[1];
                                String msgToSend = "yoursuccessor"+","+portStr;
                                successor = sepMsg[1];
                                //updateMaxMin(portHash,sepMsg[1]);
                                out.println("");
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor,"newnode");
                            }
                            else {
                                if(sucHash.compareTo(portHash) > 0 && portHash.compareTo(myHash) > 0) {
                                    portToSend = successor;
                                    String msgToSend = "yoursuccessor"+","+successor;
                                    successor = sepMsg[1];
                                    //updateMaxMin(portHash,sepMsg[1]);
                                    out.println("");
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, sepMsg[1],"newnode");
                                }
                                else if(sucHash.compareTo(myHash) < 0 &&
                                        ((portHash.compareTo(myHash) >0)|| (portHash.compareTo(sucHash) < 0))) {
                                    portToSend = successor;
                                    String msgToSend = "yoursuccessor"+","+successor;
                                    successor = sepMsg[1];
                                    //updateMaxMin(portHash,sepMsg[1]);
                                    out.println("");
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, sepMsg[1],"newnode");
                                }
                                else {
                                    String msgToSend = "newnode"+","+sepMsg[1];
                                    out.println("");
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor,"newnode");
                                    //portToSend = sendMsg(msgToSend,successor);
                                    portToSend = "";
                                }
                            }
                        }catch(NoSuchAlgorithmException ex) {
                            Log.e(TAG,"No such alg");
                        }
                    }
                    else if(sepMsg[0].equals("join")) {
                        if(!predecessor.equals("")) {
                            out.println(predecessor);
                        }
                        else {
                            out.println(portStr);
                        }
                        predecessor = sepMsg[1];
                    }
                    else if(sepMsg[0].equals("deleteall")) {
                        SQLiteDatabase dbdel = mOpenHelper.getWritableDatabase();
                        mOpenHelper.onCreate(dbdel);
                        String msgToSend = "deleteall"+","+sepMsg[1];
                        out.println("");
                        if(!portStr.equals(sepMsg[1])) {
                            dbdel.delete("main",null,null);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successor, "delete");
                        }

                    }
                    else if(sepMsg[0].equals("delete")) {
                        SQLiteDatabase dbdel = mOpenHelper.getWritableDatabase();
                        mOpenHelper.onCreate(dbdel);
                        out.println("");
                        String selection =  sepMsg[1];
                        String kID = genHash(selection);
                        String prvId = genHash(predecessor);
                        int rwsdel = 0;
                        if(kID.compareTo(prvId) > 0 && kID.compareTo(myId) <= 0) {
                            rwsdel = dbdel.delete("main","key=?",new String[]{selection});
                        }
                        else if(myId.compareTo(prvId) < 0 && ((kID.compareTo(myId) < 0) || (kID.compareTo(prvId) > 0))) {
                            rwsdel = dbdel.delete("main","key=?",new String[]{selection});
                        }
                        else {
                            String msg = "delete"+","+selection + "," +sepMsg[2];
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, successor,"delete");
                        }
                        Log.e(TAG,"Rows deleted from "+portStr+ " are "+rwsdel);
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "ServerTask failed IOException");
                return null;
            } catch(NoSuchAlgorithmException ex) {
                Log.e(TAG,"No such alg");
                return null;
            }
        }
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}

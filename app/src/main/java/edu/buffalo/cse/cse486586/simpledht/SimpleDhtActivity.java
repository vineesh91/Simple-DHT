package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SimpleDhtActivity extends Activity {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private static final int TEST_CNT = 50;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        //LDump
        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    TextView tv = (TextView) findViewById(R.id.textView1);
                    tv.setText("");
                    ContentResolver cval = getContentResolver();
                    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                    Cursor resultCursor = cval.query(uri, null, "@", null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }
                    else {
                        while(resultCursor.moveToNext()) {
                            tv.append(resultCursor.getString(0) +" "+resultCursor.getString(1)+"\n");
                        }
                    }
                }catch(Exception ex) {
                    Log.e(TAG, "LDump error");
                }
            }
        });

        //GDump
        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    TextView tv = (TextView) findViewById(R.id.textView1);
                    tv.setText("");
                    ContentResolver cval = getContentResolver();
                    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                    Cursor resultCursor = cval.query(uri, null, "*", null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }
                    else {
                        while(resultCursor.moveToNext()) {
                            tv.append(resultCursor.getString(0) +" "+resultCursor.getString(1)+"\n");
                        }
                    }
                }catch(Exception ex) {
                    Log.e(TAG, "GDump error");
                }
            }
        });
    }

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

            return;
        }
    }



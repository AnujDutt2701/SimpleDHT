package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

public class SimpleDhtContract {

    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private SimpleDhtContract() {
    }

    /* Inner class that defines the table contents */
    public static class KeyValueEntry implements BaseColumns {
        public static final String TABLE_NAME = "entry";
        public static final String COLUMN_NAME_KEY = "key_string";
        public static final String COLUMN_NAME_VALUE = "value_string";
    }
}

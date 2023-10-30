package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.categories.Proj99Tests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.ByteBuffer;

import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({Proj99Tests.class, SystemTests.class})
public class TestDateDataBox {
    private Date nowDateObj;
    private String nowDateString;
    private long nowDateLong;
    private Date oldDateObj;
    private String oldDateString;
    private long oldDateLong;

    @Before
    public void setup() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        nowDateObj = new Date();
        nowDateString = sdf.format(nowDateObj);
        nowDateLong = nowDateObj.getTime();

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(1999, 01, 23);
        oldDateObj = cal.getTime();
        oldDateString = sdf.format(oldDateObj);
        oldDateLong = oldDateObj.getTime();
    }

    @Test
    public void testType() {
        assertEquals(Type.dateType(), new DateDataBox(nowDateObj).type());
        assertEquals(Type.dateType(), new DateDataBox(nowDateLong).type());
        assertEquals(Type.dateType(), new DateDataBox(nowDateString).type());
    }

    @Test(expected = RuntimeException.class)
    public void testGetBool() {
        new DateDataBox(nowDateObj).getBool();
    }

    @Test(expected = RuntimeException.class)
    public void testGetInt() {
        new DateDataBox(nowDateObj).getInt();
    }

    @Test(expected = RuntimeException.class)
    public void testGetLong() {
        new DateDataBox(nowDateObj).getLong();
    }

    @Test(expected = RuntimeException.class)
    public void testGetFloat() {
        new DateDataBox(nowDateObj).getFloat();
    }

    @Test(expected = RuntimeException.class)
    public void testGetString() {
        new DateDataBox(nowDateObj).getString();
    }

    private void testToAndFromBytesSingle(DateDataBox d) {
        byte[] bytes = d.toBytes();
        assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.dateType()));
    }

    @Test
    public void testToAndFromBytes() {
        testToAndFromBytesSingle(new DateDataBox(nowDateObj));
        testToAndFromBytesSingle(new DateDataBox(nowDateString));
        testToAndFromBytesSingle(new DateDataBox(nowDateLong));

        testToAndFromBytesSingle(new DateDataBox(oldDateObj));
        testToAndFromBytesSingle(new DateDataBox(oldDateString));
        testToAndFromBytesSingle(new DateDataBox(oldDateLong));
    }

    private void testEqualsSingle(DateDataBox d1, DateDataBox d2) {
        assertEquals(d1, d1);
        assertEquals(d2, d2);
        assertNotEquals(d1, d2);
        assertNotEquals(d2, d1);
    }
    @Test
    public void testEquals() {
        testEqualsSingle(new DateDataBox(nowDateObj), new DateDataBox(oldDateObj));
        testEqualsSingle(new DateDataBox(nowDateString), new DateDataBox(oldDateString));
        testEqualsSingle(new DateDataBox(nowDateLong), new DateDataBox(oldDateLong));
    }

    private void testCompareToSingle(DateDataBox now, DateDataBox old) {
        assertTrue(old.compareTo(now) < 0);
        assertTrue(now.compareTo(old) > 0);
        assertTrue(old.compareTo(old) == 0);
        assertTrue(now.compareTo(now) == 0);
    }
    @Test
    public void testCompareTo() {
        testCompareToSingle(new DateDataBox(nowDateObj), new DateDataBox(oldDateObj));
        testCompareToSingle(new DateDataBox(nowDateString), new DateDataBox(oldDateString));
        testCompareToSingle(new DateDataBox(nowDateLong), new DateDataBox(oldDateLong));
    }
}

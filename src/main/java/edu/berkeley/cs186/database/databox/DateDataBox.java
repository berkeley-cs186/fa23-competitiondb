package edu.berkeley.cs186.database.databox;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;

public class DateDataBox extends DataBox {
    // We just store date as a long
    private Date date;
    public DateDataBox(Date date) {
        this.date = date;
    }

    public DateDataBox(long dateLong) {
        this.date = new Date(dateLong);
    }
    public DateDataBox(String dateString) {
        this.date = getDate(dateString);
    }

    private static Date getDate(String dateString) {
        try {
            SimpleDateFormat sdf = getDateFormatter();
            Date date = sdf.parse(dateString);
            return date;
        }
        catch (ParseException e) {
            throw new RuntimeException("Could not parse date: " + dateString);
        }
    }

    private static SimpleDateFormat getDateFormatter() {
        // Not very generalizable but good enough for now.
        return new SimpleDateFormat("yyyy-MM-dd");
    }

    @Override
    public Type type() {
        return Type.dateType();
    }

    @Override
    public TypeId getTypeId() { return TypeId.DATE; }

    @Override
    public Date getDate() {
        return this.date;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Long.BYTES).putLong(this.date.getTime()).array();
    }

    @Override
    public String toString() {
        return this.date.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DateDataBox)) {
            return false;
        }
        DateDataBox d = (DateDataBox) o;
        return this.date.equals(d.date);
    }

    @Override
    public int hashCode() {
        return new Long(date.getTime()).hashCode();
    }

    @Override
    public int compareTo(DataBox d) {
        if (!(d instanceof DateDataBox)) {
            String err = String.format("Invalid comparison between %s and %s.",
                    this.toString(), d.toString());
            throw new IllegalArgumentException(err);
        }
        DateDataBox otherDate = (DateDataBox) d;
        return this.date.compareTo(otherDate.date);
    }

    private Calendar getCalendar() {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTime(this.date);
        return cal;
    }

    public int extract(String part) {
        String lowerPart = part.toLowerCase();
        Calendar cal = this.getCalendar();
        switch (lowerPart) {
            case "year": return cal.get(Calendar.YEAR);
            case "month": return cal.get(Calendar.MONTH);
            case "day": return cal.get(Calendar.DAY_OF_MONTH);
            default: throw new RuntimeException(part + " is not a valid date subpart. Must be one of month, year, or day.");
        }
    }
}

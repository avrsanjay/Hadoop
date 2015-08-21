package com.cloudwick.hadoop.Task5;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
public class TextTriplet implements WritableComparable<TextTriplet> {
 
    private Text ip_address;
    private Text req_page;
    private Text time_stamp;
 
    public TextTriplet(Text first, Text second, Text third) {
        set(first, second, third);
    }
 
    public TextTriplet() {
        set(new Text(), new Text(), new Text());
    }
 
    public TextTriplet(String first, String second, String third) {
        set(new Text(first), new Text(second), new Text(third));
    }
 
    public Text getIp() {
        return ip_address;
    }
 
    public Text getReqPage() {
        return req_page;
    }
    
    public Text getTimeStamp() {
        return time_stamp;
    }
 
    public void set(Text first, Text second, Text third) {
        this.ip_address = first;
        this.req_page = second;
        this.time_stamp = third;
    }
    
    public void readFields(DataInput in) throws IOException {
        ip_address.readFields(in);
        req_page.readFields(in);
        time_stamp.readFields(in);
    }
 
    public void write(DataOutput out) throws IOException {
        ip_address.write(out);
        req_page.write(out);
        time_stamp.write(out);
    }
 
    public String toString() {
        return ip_address + " " + req_page + " " + time_stamp;
    }
 
    public int compareTo(TextTriplet tt) {
        int res1 = ip_address.compareTo(tt.ip_address);
        if(res1 != 0)
        	return res1;
        else
        {
        	int res2 = req_page.compareTo(tt.req_page);
        	if(res2 != 0)
            	return res2;
        	else
        		return time_stamp.compareTo(tt.time_stamp);
        }
    }
 
    @Override
    public int hashCode(){
        return ip_address.hashCode()*163 + req_page.hashCode()*337 + time_stamp.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TextTriplet)
        {
            TextTriplet tt = (TextTriplet) o;
            return ip_address.equals(tt.ip_address) && req_page.equals(tt.req_page) && time_stamp.equals(tt.time_stamp);
        }
        return false;
    }
}

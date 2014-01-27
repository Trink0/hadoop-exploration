package it.xpeppers.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Created by Trink0 on 20/01/14.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
    @Override
    public boolean equals(Object o){
        if (!(o instanceof TextArrayWritable))
            return false;
        TextArrayWritable val = (TextArrayWritable) o;
        Writable[] localList = this.get();
        Writable[] compareList = val.get();
        for (int i = 0; i < localList.length; i++){
            if (!localList[i].equals(compareList[i]))
                return false;
        }
        return true;
    }
    @Override
    public String toString(){
        return get().toString();
    }
    @Override
    public int hashCode(){
        int hash = 0;
        for (Writable text: get()){
            hash += text.hashCode() * 171;
        }
        return hash;
    }
}

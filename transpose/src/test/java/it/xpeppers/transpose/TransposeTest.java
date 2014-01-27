package it.xpeppers.transpose;

import it.xpeppers.transpose.TransposeMapper;
import it.xpeppers.transpose.TransposeReducer;
import it.xpeppers.utils.TextArrayWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by Trink0 on 16/01/14.
 */
public class TransposeTest {
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, ArrayWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, ArrayWritable> mapReduceDriver;

    private void runAndAssertEquals(String text1, String text2, String text3) throws IOException {
        List<Pair<Text,ArrayWritable>> output = reduceDriver.run();

        assertOutputIsEquals(text1, text2, text3, output.get(0));
    }

    private void assertOutputIsEquals(String text1, String text2, String text3, Pair<Text, ArrayWritable> output) {
        ArrayWritable expectedValues = new TextArrayWritable();
        Text[] textValues = new Text[] {new Text(text1), new Text(text2), new Text(text3)};
        expectedValues.set(textValues);
        Pair<Text,ArrayWritable> field = output;
        assertEquals(field.getSecond(), expectedValues);
    }

    @Before
    public void setUp() {
        TransposeMapper mapper = new TransposeMapper();
        mapper.setSeparator(",");
        TransposeReducer reducer = new TransposeReducer();
        reducer.setSeparator(",");
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    @Test
    public void mapTestOneInput() throws IOException { //GK=SO_CLOB[1]/ACC_BASE[1] CK=CHANNEL_TYPE
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/ACC_BASE[1]"), new Text("CHANNEL_TYPE,100"));
        mapDriver.runTest();
    }
    @Test
    public void mapTestTwoInput() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/ACC_BASE[1]"), new Text("CHANNEL_TYPE,100"));
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CD_ORDINE,200"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/ACC_BASE[1]"), new Text("CD_ORDINE,200"));
        mapDriver.runTest();
    }
    @Test
    public void mapTestMoreInputDifferentKeys() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/ACC_BASE[1]"), new Text("CHANNEL_TYPE,100"));
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CD_ORDINE,200"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/ACC_BASE[1]"), new Text("CD_ORDINE,200"));
        mapDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/CONN_VIRTCH[1],ACC_BASE,MCRDN,1000"));
        mapDriver.withOutput(new Text("SO_CLOB[1]/CONN_VIRTCH[1]"), new Text("MCRDN,1000"));
        mapDriver.runTest();
    }
    @Test
    public void reduceTestFirstValue() throws IOException {
        List<Text> listOfValues = new ArrayList<Text>();
        listOfValues.add(new Text("CHANNEL_TYPE,100"));
        reduceDriver.withInput(new Text("Gk"), listOfValues);

        runAndAssertEquals("100", "", "");
    }
    @Test
    public void reduceTestSecondValue() throws IOException {
        List<Text> listOfValues = new ArrayList<Text>();
        listOfValues.add(new Text("CD_ORDINE,200"));
        reduceDriver.withInput(new Text("Gk"), listOfValues);

        runAndAssertEquals("", "200", "");
    }
    @Test
    public void reduceTestComposedValue() throws IOException {
        List<Text> listOfValues = new ArrayList<Text>();
        listOfValues.add(new Text("CHANNEL_TYPE,100"));
        listOfValues.add(new Text("CD_ORDINE,200"));
        reduceDriver.withInput(new Text("Gk"), listOfValues);

        runAndAssertEquals("100", "200", "");
    }
    @Test
    public void reduceTestComposedAlternateValue() throws IOException {
        List<Text> listOfValues = new ArrayList<Text>();
        listOfValues.add(new Text("CHANNEL_TYPE,100"));
        listOfValues.add(new Text("MCRDN,300"));
        reduceDriver.withInput(new Text("Gk"), listOfValues);

        runAndAssertEquals("100", "", "300");
    }
    @Test
    public void mapReduceKeyOneValue() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        List<Pair<Text,ArrayWritable>> output = mapReduceDriver.run();

        assertOutputIsEquals("100", "", "", output.get(0));
    }
    @Test
    public void mapReduceKeyTwoValue() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CD_ORDINE,200"));
        List<Pair<Text,ArrayWritable>> output = mapReduceDriver.run();

        assertEquals(1, output.size());
        assertOutputIsEquals("100", "200", "", output.get(0));
    }
    @Test
    public void mapReduceTwoKeyOneValue() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/CONN_VIRTCH[1],ACC_BASE,CD_ORDINE,200"));
        List<Pair<Text,ArrayWritable>> output = mapReduceDriver.run();

        assertEquals(2, output.size());
        assertOutputIsEquals("100", "", "", output.get(0));
        assertOutputIsEquals("", "200", "", output.get(1));
    }
    @Test
    public void mapReduceTwoKeyTwoValue() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CD_ORDINE,200"));
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/ACC_BASE[1],ACC_BASE,CHANNEL_TYPE,100"));
        mapReduceDriver.withInput(new LongWritable(), new Text("2013-12-02 15:34,I,NG000,SO_CLOB[1]/CONN_VIRTCH[1],ACC_BASE,MCRDN,300"));
        List<Pair<Text,ArrayWritable>> output = mapReduceDriver.run();

        assertEquals(2, output.size());
        assertOutputIsEquals("100", "200", "", output.get(0));
        assertOutputIsEquals("", "", "300", output.get(1));
    }
}

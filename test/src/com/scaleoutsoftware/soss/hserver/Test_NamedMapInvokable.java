package com.scaleoutsoftware.soss.hserver;

import com.scaleoutsoftware.soss.client.*;
import com.scaleoutsoftware.soss.client.map.*;
import com.scaleoutsoftware.soss.client.map.impl.DefaultSerializer;
import org.apache.hadoop.io.*;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
 * This class tests NamedMapInvokables with Writable objects.
 */
public class Test_NamedMapInvokable implements Serializable {
    public Test_NamedMapInvokable() {
    }

    public static class Summ extends NamedMapInvokable<String, Integer, String, Integer> {
        @Override
        public Integer eval(String s, Integer integer, String s2) throws InvokeException {
            return integer;
        }

        @Override
        public Integer merge(Integer firstResult, Integer secondResult, String s) throws InvokeException {
            firstResult += secondResult;
            return firstResult;
        }
    }



    @Test
    public void testBasic() throws Exception {
        NamedMap<String, Integer> map = NamedMapFactory.getMap("myMap");
        map.clear();
        InvocationGrid grid = (new InvocationGridBuilder("myGrid" + System.currentTimeMillis())).addClass(Test_NamedMapInvokable.class).load();
        map.setInvocationGrid(grid);

        for (int i = 0; i < 1000; i++) {
            map.put("" + i, 1);
        }
        assertEquals(1000, (int) map.invoke(new Summ(), ""));
        grid.unload();
    }

    @Test
    public void testQuery() throws Exception {
        NamedMap<String, Integer> map = NamedMapFactory.getMap("myMap");
        map.clear();

        InvocationGrid grid = (new InvocationGridBuilder("myGrid" + System.currentTimeMillis())).setLibraryPath("hdp2-yarn").addClass(Test_NamedMapInvokable.class).load();
        map.setInvocationGrid(grid);

        for (int i = 0; i < 10000; i++) {
            map.put("" + i, i);
        }

        Iterator<String> keys = map.executeParallelQuery(new StringIntegerQueryCondition()).iterator();


        int count = 0;
        while (keys.hasNext()) {
            assertTrue(map.get(keys.next()) % 2 == 0);
            count++;
        }
        assertEquals(count, 5000);
        grid.unload();
    }


    @Test
    public void testBigInvocationWithMapModified() throws Exception {
        int numKeys = 10000000;
        long time;
        NamedMap<Text, IntWritable> map = NamedMapFactory.getMap("myMapW",
                new WritableSerializer<Text>(Text.class),
                new WritableSerializer<IntWritable>(IntWritable.class));
        map.clear();
        InvocationGrid grid = (new InvocationGridBuilder("myGrid" + System.currentTimeMillis())).setLibraryPath("hadoop-1.2.1").addClass(Test_NamedMapInvokable.class).load();
        map.setInvocationGrid(grid);

        time = System.currentTimeMillis();
        BulkLoader<Text, IntWritable> put = map.getBulkLoader();
        for (int i = 0; i < numKeys; i++) {
            put.put(new Text("" + i), new IntWritable(1));
        }
        put.close();
        System.out.println("Bulk loaded completed in: " + (System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        List<String> result = map.invoke(new TextIntWritableListNamedMapInvokable(), "");
        System.out.println("Parallel op in: " + (System.currentTimeMillis() - time));
        assertEquals(result.size(), numKeys);


        assertEquals(map.size(), numKeys);

        time = System.currentTimeMillis();
        map.invoke(new TextIntWritableNamedMapInvokable(), "");


        System.out.println("Validation op in: " + (System.currentTimeMillis() - time));
        grid.unload();
    }

    public static class FixedSizeValueSerializer extends CustomSerializer<Integer> {

        @Override
        public void serialize(OutputStream out, Integer o) throws ObjectNotSupportedException, IOException {
            out.write(o.byteValue());
        }

        @Override
        public Integer deserialize(InputStream in) throws ObjectNotSupportedException, IOException, ClassNotFoundException {
            return in.read();
        }

        @Override
        public int getSize() {
            return 1;
        }
    }

    public static class TestCondition implements QueryCondition<String, Integer> {
        @Override
        public boolean check(String key, Integer value) throws InvokeException {
            if(!(""+value).equals(key))
            {
                throw new InvokeException("ASSERT");
            }
            return true;
        }
    }

    @Test
    public void testFixedSizeValue() throws Exception
    {
        NamedMap<String, Integer> namedMap = NamedMapFactory.getMap("map", new DefaultSerializer() , new FixedSizeValueSerializer());
        namedMap.clear();
        namedMap.put("key",1);
        assertEquals(new Integer(1), namedMap.get("key"));
        namedMap.put("key",2);
        assertEquals(new Integer(2), namedMap.get("key"));
        namedMap.clear();
        BulkLoader<String, Integer> put = namedMap.getBulkLoader();
        for(int i=0; i<200; i++)
        {
            put.put(""+i, i);
        }
        put.close();
        for(int i=0; i<200; i++)
        {
            assertEquals(i,(int)namedMap.get(""+i));
        }

        InvocationGrid grid = (new InvocationGridBuilder("grid"+System.currentTimeMillis())).addClass(this.getClass()).load(true);
        namedMap.setInvocationGrid(grid);

        Iterator<String> keys = namedMap.executeParallelQuery(new TestCondition()).iterator();
        for(int i=0; i<200; i++)
        {
            assertTrue(keys.hasNext());
            String key = keys.next();
            assertEquals(key, ""+namedMap.get(key));
        }
        grid.unload();
    }


    public static class CacheInvokable implements Invokable<OBJ, String, Integer> {


        @Override
        public Integer eval(OBJ toEval, String param, EvalArgs<OBJ> args) throws InvokeException, InterruptedException {
            return 0;
        }
        @Override
        public Integer merge(Integer first, Integer second) throws InvokeException, InterruptedException {
            return 0;
        }
    }

    public static class MapInvokable extends NamedMapInvokable<UUID, OBJ, String, Integer> {
        @Override
        public Integer eval(UUID uuid, OBJ integer, String s) throws InvokeException {
            return 0;
        }
        @Override
        public Integer merge(Integer firstResult, Integer secondResult, String s) throws InvokeException {
            return 0;
        }
    }

    public static class OBJ implements Serializable
    {
        public OBJ(Integer i) {
            this.a = i+1;
            this.b = i+2;
            this.c = i+3;
        }

        Integer a = 0;
        Integer b = 0;
        Integer c = 0;
    }

    //@Test
    public void testCacheVsMap() throws Exception {
        int NUMKEYS = 100000;
        int NUMRUNS = 10;

        NamedMap<UUID, OBJ> map = NamedMapFactory.getMap("myMap");


        NamedCache cache = CacheFactory.getCache("myCache");

        cache.setAllowClientCaching(true);

        InvocationGrid grid = (new InvocationGridBuilder("myGrid" + System.currentTimeMillis())).addClass(Test_NamedMapInvokable.class).setNewClassLoader(false).setAllowClientCaching(true).load();

        map.setInvocationGrid(grid);
        cache.setInvocationGrid(grid);

        map.clear();
        cache.clear();
        for (int i = 0; i < NUMKEYS; i++) {
            UUID randomUUID = UUID.randomUUID();
            map.put(randomUUID, new  OBJ(i));
            cache.put(randomUUID, new  OBJ(i));
        }

        System.out.println("Objects created");
        long timer, timeMap = 0, timeCache = 0;

        for (int i = 0; i < NUMRUNS; i++) {
            timer = System.currentTimeMillis();
            map.invoke(new MapInvokable(), "");
            timeMap += (System.currentTimeMillis() - timer);
            System.out.println("MAP DONE:"+timeMap);

            timer = System.currentTimeMillis();
            assertEquals(NUMKEYS, cache.invoke(CacheInvokable.class, new NamedCacheFilter(), "", TimeSpan.INFINITE_TIMEOUT).getNumSuccessful());
            timeCache += (System.currentTimeMillis() - timer);
            System.out.println("CACHE DONE:"+timeCache);
        }
        System.out.println("Client cache: Map vs Cache: Runs = " + NUMRUNS + " Keys = " + NUMKEYS + " Map time = " + timeMap / NUMRUNS + " Cache time = " + (double)timeCache / NUMRUNS + " Ratio =" + timeCache / timeMap);

        grid.unload();

        grid = (new InvocationGridBuilder("myGrid" + System.currentTimeMillis())).setNewClassLoader(false).addClass(Test_NamedMapInvokable.class).setAllowClientCaching(false).load();

        map.setInvocationGrid(grid);
        cache.setInvocationGrid(grid);

        timeMap = 0;
        timeCache = 0;

        for (int i = 0; i < NUMRUNS; i++) {
            timer = System.currentTimeMillis();
            map.invoke(new MapInvokable(), "");
            timeMap += (System.currentTimeMillis() - timer);
            System.out.println("MAP DONE:"+timeMap);

            timer = System.currentTimeMillis();
            assertEquals(NUMKEYS, cache.invoke(CacheInvokable.class, new NamedCacheFilter(), "", TimeSpan.INFINITE_TIMEOUT).getNumSuccessful());
            timeCache += (System.currentTimeMillis() - timer);
            System.out.println("CACHE DONE:"+timeCache);
        }
        System.out.println("No client cache: Map vs Cache: Runs = " + NUMRUNS + " Keys = " + NUMKEYS + " Map time = " + timeMap / NUMRUNS + " Cache time = " + (double)timeCache / NUMRUNS + " Ratio =" + timeCache / timeMap);

        grid.unload();
    }

    @Test
    public void testFixedSizeValuesIntWritable() throws Exception     {

        NamedMap<String, IntWritable> namedMap = NamedMapFactory.getMap("map", new DefaultSerializer() , new WritableSerializer(IntWritable.class));
        namedMap.clear();
        namedMap.put("key",new IntWritable(1));
        assertEquals(1, namedMap.get("key").get());
        namedMap.put("key",new IntWritable(2));
        assertEquals(2, namedMap.get("key").get());
        namedMap.clear();
        BulkLoader put = namedMap.getBulkLoader();
        for(int i=0; i<200; i++)
        {
            put.put(""+i, new IntWritable(i));
        }
        put.close();
        for(int i=0; i<200; i++)
        {
            assertEquals(i,namedMap.get(""+i).get());
        }
    }
    @Test
    public void testFixedSizeValuesLongWritable() throws Exception         {

        NamedMap<String, LongWritable> namedMap = NamedMapFactory.getMap("map", new DefaultSerializer() , new WritableSerializer(LongWritable.class));
        namedMap.clear();
        namedMap.put("key",new LongWritable(1));
        assertEquals(1, namedMap.get("key").get());
        namedMap.put("key",new LongWritable(2));
        assertEquals(2, namedMap.get("key").get());
        namedMap.clear();
        BulkLoader put = namedMap.getBulkLoader();
        for(int i=0; i<200; i++)
        {
            put.put(""+i, new LongWritable(i));
        }
        put.close();
        for(int i=0; i<200; i++)
        {
            assertEquals(i,namedMap.get(""+i).get());
        }
    }

    @Test
    public void testFixedSizeValuesByteWritable() throws Exception {
        NamedMap<String, ByteWritable> namedMap = NamedMapFactory.getMap("map", new DefaultSerializer(), new WritableSerializer(ByteWritable.class));
        namedMap.clear();
        namedMap.put("key", new ByteWritable((byte) 1));
        assertEquals(1, namedMap.get("key").get());
        namedMap.put("key", new ByteWritable((byte) 2));
        assertEquals(2, namedMap.get("key").get());
        namedMap.clear();
        BulkLoader put = namedMap.getBulkLoader();
        for (int i = 0; i < 100; i++) {
            put.put("" + i, new ByteWritable((byte) i));
        }
        put.close();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, namedMap.get("" + i).get());
        }
    }

    @Test
    public void testFixedSizeValuesNullWritable() throws Exception {
        NamedMap<String, NullWritable> namedMap = NamedMapFactory.getMap("map", new DefaultSerializer(), new WritableSerializer(NullWritable.class));
        namedMap.clear();
        namedMap.put("key", NullWritable.get());
        assertEquals(NullWritable.get(), namedMap.get("key").get());
        namedMap.put("key", NullWritable.get());
        assertEquals(NullWritable.get(), namedMap.get("key").get());
        namedMap.clear();
        BulkLoader put = namedMap.getBulkLoader();
        for (int i = 0; i < 100; i++) {
            put.put("" + i,  NullWritable.get());
        }
        put.close();
        for (int i = 0; i < 100; i++) {
            assertEquals( NullWritable.get(), namedMap.get("" + i).get());
        }
    }


    private static class TextIntWritableListNamedMapInvokable extends NamedMapInvokable<Text, IntWritable, Object, List<String>> {

        @Override
        public List<String> evalPartialKeySet(NamedMapInvokableContext<Text, IntWritable> context, Object o) throws InvokeException {
            List<String> list = new ArrayList<String>();
            while (context.next()) {
                list.add(context.getKey().toString());
                IntWritable value = context.getValue();
                value.set(value.get() + 1);
                context.updateCurrentValue(value);
            }
            return list;
        }

        @Override
        public List<String> merge(List<String> firstResult, List<String> secondResult, Object o) throws InvokeException {
            firstResult.addAll(secondResult);
            return firstResult;
        }
    }

    private static class StringIntegerQueryCondition implements QueryCondition<String, Integer> {
        @Override
        public boolean check(String key, Integer value) throws InvokeException {
            return value % 2 == 0;
        }
    }

    private static class TextIntWritableNamedMapInvokable extends NamedMapInvokable<Text, IntWritable, Object, Object> {
        @Override
        public Object eval(Text text, IntWritable intWritable, Object o) throws InvokeException {
            if (intWritable.get() != 2) {
                throw new InvokeException("Check failed");
            }
            return null;
        }

        @Override
        public Object merge(Object firstResult, Object secondResult, Object o) throws InvokeException {
            return null;
        }
    }
}

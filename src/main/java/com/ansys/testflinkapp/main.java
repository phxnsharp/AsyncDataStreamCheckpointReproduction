package com.ansys.testflinkapp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 *
 */
public class main
{
   public static final int SIZE = 240;
   
   public static void main(String[] args) throws Exception 
   {
      final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
      
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      
      env.enableCheckpointing(1000);
      //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
      env.getCheckpointConfig().enableUnalignedCheckpoints();
      
      // make parameters available in the web interface
      env.getConfig().setGlobalJobParameters(params);
      
      Class<Map<String, Double>> bla = (Class)Map.class;
      
      DataStreamSource<Map<String, Double>> inputs = 
         env.fromCollection(new InputGenerator(), bla);

      DataStream<Map<String, Double>> results =
         AsyncDataStream.unorderedWait(inputs, new QuadFunc(), 5, TimeUnit.SECONDS, 
            SIZE/60+1); //chosen to make the whole operation take about 1 minute
      
      results
         .print().name("print-sink");
      env.execute("Flink Test App");
   }

   private static class InputGenerator implements Iterator<Map<String, Double>>, Serializable
   {
      private int _count = 0;
         
      @Override
      public boolean hasNext()
      {
         return _count < SIZE;
      }

      @Override
      public Map<String, Double> next() 
      {
         _count++;
         Map<String, Double> result = new HashMap<>();
         result.put("x", _count * 1.2);
         result.put("count", _count*1.0);
         return result;
      }
   }
   
   private static class QuadFunc extends RichAsyncFunction<Map<String, Double>, Map<String, Double>> 
   {
      private transient ScheduledExecutorService sleeper;

      @Override
      public void open(Configuration parameters) throws Exception 
      {
         sleeper = Executors.newScheduledThreadPool(1);
      }
    
      @Override
      public void close()
      {
         if ( sleeper != null )
         {
            sleeper.shutdown();
            sleeper = null;
         }
      }

      @Override
      public void asyncInvoke(Map<String, Double> input, ResultFuture<Map<String, Double>> callback) throws Exception
      {
         long start = System.currentTimeMillis();
         sleeper.schedule(() -> 
         {
            double x = input.get("x");
            Map<String, Double> result = new HashMap<>();
            result.put("y", x*x);
            result.put("count", input.get("count"));
            callback.complete(List.of(result));
         }, 1, TimeUnit.SECONDS);
         
         //Verify that we are not blocking this thread.
         if ( (System.currentTimeMillis() - start) > 100 )
         {
            throw new Exception("We are blocking the asyncInvoke thread illegally");
         }
      }
   }
}

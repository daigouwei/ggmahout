package com.mahout.gg.inaction.ch06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * 计算共现关系的Reducer
 * Created by guowei on 2018/6/25.
 */
public class UserVectorToCooccurrenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, VectorWritable> {
    @Override
    public void reduce(IntWritable itemIndex1, Iterable<IntWritable> itemIndex2s, Context context)
            throws IOException, InterruptedException {
        Vector cooccurrenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for (IntWritable intWritable : itemIndex2s) {
            int itemIndex2 = intWritable.get();
            cooccurrenceRow.set(itemIndex2, cooccurrenceRow.get(itemIndex2) + 1.0);
        }
        context.write(itemIndex1, new VectorWritable(cooccurrenceRow));
    }
}

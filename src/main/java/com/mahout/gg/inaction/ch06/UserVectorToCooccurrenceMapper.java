package com.mahout.gg.inaction.ch06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * 计算共现关系的Mapper
 * Created by guowei on 2018/6/25.
 */
public class UserVectorToCooccurrenceMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, IntWritable> {
    public void map(VarLongWritable userID, VectorWritable userVector, Context context)
            throws IOException, InterruptedException {
        Iterator<Vector.Element> it = userVector.get().nonZeroes().iterator();
        while (it.hasNext()) {
            int index1 = it.next().index();
            Iterator<Vector.Element> it2 = userVector.get().nonZeroes().iterator();
            while (it2.hasNext()) {
                int index2 = it2.next().index();
                context.write(new IntWritable(index1), new IntWritable(index2));
            }
        }
    }
}

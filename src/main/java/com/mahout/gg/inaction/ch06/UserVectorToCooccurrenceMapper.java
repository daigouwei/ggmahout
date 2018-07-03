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
 * Map函数根据用户的偏好来决定所有的共现关系，并为每一个共现关系生成一个物品ID对--物品ID对应到物品ID。无论是从一个物品ID到另一个的对应关系，
 * 或截然相反的对应关系，都会被记录下来。所以代码中会出现两次迭代器循环语句。
 * Created by guowei on 2018/6/25.
 */
public class UserVectorToCooccurrenceMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, IntWritable> {
    @Override
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

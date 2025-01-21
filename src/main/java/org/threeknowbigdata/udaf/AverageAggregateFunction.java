package org.threeknowbigdata.udaf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;

// 定义一个简单的 UDAF，用于计算输入元素的平均值
public class AverageAggregateFunction extends AggregateFunction<Double, AverageAggregateFunction.AverageAccumulator> {

    // 1 定义一个 中间结果类型的类 Accumulator，存放聚合的中间结果
    public static class AverageAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // 2 创建初始的聚合状态,重写 createAccumulator 方法
    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    // 3 创建 accumulate 方法，将输入元素添加到聚合状态中，
    public void accumulate(AverageAccumulator accumulator, Long value) {
        if (value!= null) {
            accumulator.sum += value;
            accumulator.count++;
        }
    }

    // 4 重写getValue, 计算最终结果
    @Override
    public Double getValue(AverageAccumulator accumulator) {
        if (accumulator.count == 0) {
            return null;
        }
        return ((double) accumulator.sum) / accumulator.count;
    }

    // 合并不同分区的聚合状态
    public void merge(AverageAccumulator accumulator, Iterable<AverageAccumulator> its) {
        for (AverageAccumulator otherAcc : its) {
            accumulator.sum += otherAcc.sum;
            accumulator.count += otherAcc.count;
        }
    }

    // 重置聚合状态
    public void resetAccumulator(AverageAccumulator accumulator) {
        accumulator.sum = 0;
        accumulator.count = 0;
    }

    // 定义输出结果的类型信息
    @Override
    public TypeInformation<Double> getResultType() {
        return TypeInformation.of(new TypeHint<Double>() {});
    }

    // 定义中间结果的类型信息
    @Override
    public TypeInformation<AverageAccumulator> getAccumulatorType() {
        return TypeInformation.of(new TypeHint<AverageAccumulator>() {});
    }
}
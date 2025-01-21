package org.threeknowbigdata.udaf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

//自定义UDAF

@FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
public class UDAFSum extends AggregateFunction<Integer, UDAFSum.SumAccumulator> {

//定义一个Accumulator，存放聚合的中间结果

    public static class SumAccumulator{
        public int sumPrice;
    }

    /** 初始化Accumulator
     * @return*/
    @Override
    public SumAccumulator createAccumulator() {
        SumAccumulator sumAccumulator = new SumAccumulator();
        sumAccumulator.sumPrice=0;
        return sumAccumulator;
    }

    /* 定义如何根据输入更新Accumulator
     * @param accumulator Accumulator
     * @param productPrice 输入
     */

    public void accumulate( SumAccumulator accumulator, int productPrice){
        accumulator.sumPrice += productPrice;
    }


// * 返回聚合的最终结果
// * @param accumulator Accumulator
// * @return

    @Override
    public Integer getValue(SumAccumulator accumulator) {
        return accumulator.sumPrice;
    }
}

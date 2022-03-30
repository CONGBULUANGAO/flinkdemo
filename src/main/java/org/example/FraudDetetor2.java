package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetetor2 extends KeyedProcessFunction<Long, Transaction, Alert> {

    ValueState<Boolean> flagState;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> valueStateDescriptor = new ValueStateDescriptor<Boolean>("flag", Boolean.TYPE);
        flagState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSamll = flagState.value();
        if(lastTransactionWasSamll != null){
            if(transaction.getAmount() > LARGE_AMOUNT){
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            flagState.clear();
        }

        if(transaction.getAmount()<SMALL_AMOUNT){
            flagState.update(true);
        }
    }
}

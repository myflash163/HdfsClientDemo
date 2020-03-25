package com.atguigu.mr.topn;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;


    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void set(long downFlow2, long upFlow2) {
        downFlow = downFlow2;
        upFlow = upFlow2;
        sumFlow = downFlow2 + upFlow2;
    }

    @Override
    public int compareTo(FlowBean bean) {

        int result;

        if (this.sumFlow > bean.getSumFlow()) {
            result = -1;
        }else if (this.sumFlow < bean.getSumFlow()) {
            result = 1;
        }else {
            result = 0;
        }

        return result;
    }
}

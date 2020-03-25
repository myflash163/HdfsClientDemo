package com.atguigu.mr.sort;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow; //上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量


    @Override
    public int compareTo(FlowBean flowBean) {
        int result;
        if (sumFlow > flowBean.getSumFlow()) {
            result = -1;
        } else if (sumFlow < flowBean.getSumFlow()) {
            result = 1;
        } else {
            result = 0;
        }
        return result;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(upFlow);
        output.writeLong(downFlow);
        output.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        upFlow = input.readLong();
        downFlow = input.readLong();
        sumFlow = input.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}

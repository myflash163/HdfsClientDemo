package com.atguigu.mr.order;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@EqualsAndHashCode
public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        super();
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean bean) {
        int result;
        if (order_id > bean.getOrder_id()) {
            result = 1;
        } else if (order_id < bean.getOrder_id()) {
            result = -1;
        } else {
            if (price < bean.getPrice()) {
                result = 1;
            } else {
                result = 0;
            }
        }
        return result;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}

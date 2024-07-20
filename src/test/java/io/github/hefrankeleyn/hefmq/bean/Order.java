package io.github.hefrankeleyn.hefmq.bean;

import com.google.common.base.MoreObjects;

/**
 * @Date 2024/7/20
 * @Author lifei
 */
public class Order {
    private Long id;
    private String item;
    private Double price;

    public Order() {}

    public Order(Long id, String item, Double price) {
        this.id = id;
        this.item = item;
        this.price = price;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(Order.class)
                .add("id", id)
                .add("item", item)
                .add("price", price)
                .toString();
    }
}

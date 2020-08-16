package com.rty.kafka.quickly.vo;

/**
 * 实体类
 *
 * @author rty
 * @since 2020-08-16
 */
public class DemoUser {
    /**
     * is
     */
    private Integer id;
    /**
     * name
     */
    private String name;

    public DemoUser(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "DemoUser{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}

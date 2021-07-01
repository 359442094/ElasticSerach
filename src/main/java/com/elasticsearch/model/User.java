package com.elasticsearch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

    public static final List<User> users = new ArrayList<User>() {
        {
            add(new User("12341","11", "男", 1,"中华人民共和国"));
            add(new User("12342","22", "男", 2,"中华人民万岁"));
            add(new User("12343","cj33", "男", 3,"中华人民共和国成立了"));
            add(new User("12344","cj44", "男", 5,"中国是个伟大的发展中国家"));
            add(new User("12345","cj55", "男", 5,"分词查询效果测试哦1"));
            add(new User("12345","cj66", "男", 6,"分词查询效果测试哦2"));
        }
    };

    private String userId;
    private String name;
    private String sex;
    private Integer age;
    private String message;

}

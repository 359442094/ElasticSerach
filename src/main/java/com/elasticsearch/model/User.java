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
            add(new User("12341","11", "男", 1));
            add(new User("12342","22", "男", 2));
            add(new User("12343","chenjie33", "男", 3));
            add(new User("12344","cj44", "男", 5));
            add(new User("12345","cj55", "男", 5));
            add(new User("12345","cj66", "男", 6));
        }
    };

    private String userId;
    private String name;
    private String sex;
    private Integer age;

}

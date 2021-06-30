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
            add(new User("12341","陈杰1", "男", 21));
            add(new User("12342","陈杰2", "男", 22));
            add(new User("12343","陈杰3", "男", 23));
            add(new User("12344","陈杰4", "男", 24));
        }
    };

    private String userId;
    private String name;
    private String sex;
    private Integer age;

}

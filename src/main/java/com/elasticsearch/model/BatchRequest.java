package com.elasticsearch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author chenjie
 * @since 2021/7/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class BatchRequest {

    private String indexName;
    private String id;
    private String source;

}

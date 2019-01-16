package com.hct.kafka.messaging.stream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KeyCountStreamConfig {

    private String sourceTopic;
    private String sinkTopic;
    private String storeName;
}

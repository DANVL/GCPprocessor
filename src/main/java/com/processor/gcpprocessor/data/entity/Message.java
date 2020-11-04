package com.processor.gcpprocessor.data.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Message {

    private String messageId;
    private String publishTime;
    private String data;
    private Map<String,String> attributes;
}

package org.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {

    public long msgId;
    public String content;

    @JsonCreator
    public Message(
        @JsonProperty("msgId") long msgId,
        @JsonProperty("content") String content
    ) {
        this.msgId = msgId;
        this.content = content;
    }

    public Message() {}
}

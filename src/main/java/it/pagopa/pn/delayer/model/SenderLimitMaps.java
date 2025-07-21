package it.pagopa.pn.delayer.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class SenderLimitMaps {
    Map<String, Integer> senderLimitMap = new HashMap<>();
    Map<String, Integer> residualSenderLimitMap = new HashMap<>();
}

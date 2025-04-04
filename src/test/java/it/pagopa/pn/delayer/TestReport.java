package it.pagopa.pn.delayer;

import lombok.Data;

import java.util.Map;

@Data
public class TestReport {
    private String deliveryDriverId;
    private String province;
    private Integer paperDeliveryRequest;
    private Integer provinceCapacity;
    private Integer startedDispatchedProvinceCapacity;
    private Integer finalDispatchedProvinceCapacity;
    private Map<String, Integer> capCapacity;
    private Map<String, Integer> startedDispatchedCapCapacity;
    private Map<String, Integer> finalDispatchedCapCapacity;
    private Long paperDeliveryReadyToSend;
    private Integer paperDeliveryExcess;
    private String executionTime;

    @Override
    public String toString() {
        return  deliveryDriverId + ',' +
                province + ',' +
                paperDeliveryRequest + ',' +
                provinceCapacity + ',' +
                startedDispatchedProvinceCapacity + ',' +
                finalDispatchedProvinceCapacity + ',' +
                capCapacity.toString().replace(",",";") + ',' +
                startedDispatchedCapCapacity.toString().replace(",",";") + ',' +
                finalDispatchedCapCapacity.toString().replace(",",";") + ',' +
                paperDeliveryReadyToSend + ',' +
                paperDeliveryExcess + ',' +
                executionTime;
    }

    public TestReport(String tuple) {
        String[] splited = tuple.split("##");
        this.deliveryDriverId = splited[0];
        this.province = splited[1];
    }
}

package it.pagopa.pn.delayer;

import lombok.Data;

import java.util.Map;

@Data
public class TestReport {
    private String unifiedDeliveryDriver;
    private String province;
    private Integer paperDeliveryRequest;
    private Integer provinceCapacity;
    private Integer startedUsedProvinceCapacity;
    private Integer finalUsedProvinceCapacity;
    private Map<String, Integer> capCapacity;
    private Map<String, Integer> startedUsedCapCapacity;
    private Map<String, Integer> finalUsedCapCapacity;
    private Integer paperDeliveryExcess;
    private String executionTime;

    @Override
    public String toString() {
        return  unifiedDeliveryDriver + ',' +
                province + ',' +
                paperDeliveryRequest + ',' +
                provinceCapacity + ',' +
                startedUsedProvinceCapacity + ',' +
                finalUsedProvinceCapacity + ',' +
                capCapacity.toString().replace(",",";") + ',' +
                startedUsedCapCapacity.toString().replace(",",";") + ',' +
                finalUsedCapCapacity.toString().replace(",",";") + ',' +
                paperDeliveryExcess + ',' +
                executionTime;
    }

    public TestReport(String tuple) {
        String[] splited = tuple.split("~");
        this.unifiedDeliveryDriver = splited[0];
        this.province = splited[1];
    }
}

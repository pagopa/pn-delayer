package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.DelayerApplication;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.inmemory.*;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestPropertySource(properties = {
        "pn.delayer.storage.impl=INMEMORY",
        "spring.application.name=PN-DELAYER-MS-BE",
        "pn.delayer.delivery-date-day-of-week=1",
        "pn.delayer.delivery-date-interval=1d",
        "pn.delayer.paper-delivery-cut-off-duration=7d"
})
@SpringJUnitConfig(classes = {DelayerApplication.class})
@Slf4j
class HighPriorityBatchServiceInMemoryTest {

    @Autowired
    private HighPriorityBatchService highPriorityService;

    @Autowired
    PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl paperDeliveryDriverUsedCapacities;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryUtils paperDeliveryUtils;



    @Test
    void testInitHighPriorityBatch() {
        highPriorityService.initHighPriorityBatch("3~GR", new HashMap<>(), Instant.now()).block();
        Instant deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(Instant.now());

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get("3~GR");
        Integer usedProvinceCapacity = paperDeliveryDriverUsedCapacities.get("3", "GR", deliveryWeek).block();
        List<String> capList = List.of("58010", "58100");
        Map<String, Integer> finalUsedCapCapacity = new HashMap<>();
        capList.forEach(cap -> {
            Integer usedCapacity = paperDeliveryDriverUsedCapacities.get("3", cap, deliveryWeek).block();
            finalUsedCapCapacity.put(cap, usedCapacity);
        });


        Assertions.assertEquals(950, highPriorityList.size());
        Assertions.assertEquals(50, usedProvinceCapacity);
        Assertions.assertEquals(50, finalUsedCapCapacity.get("58010"));
        Assertions.assertEquals(0, finalUsedCapCapacity.get("58100"));
        Assertions.assertEquals(50, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(1, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(2, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(3, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(4, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(5, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(0, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(6, ChronoUnit.DAYS)).size());
    }

}

package it.pagopa.pn.delayer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.pagopa.pn.delayer.DelayerApplication;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.inmemory.*;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = {
        "pn.delayer.storage.impl=INMEMORY",
        "spring.application.name=PN-DELAYER-MS-BE",
        "pn.delayer.delivery-date-day-of-week=1",
        "pn.delayer.high-priority-query-limit=1000",
        "pn.delayer.delivery-date-interval=1d",
        "pn.delayer.paper-delivery-cut-off-duration=7d"
})
@SpringJUnitConfig(classes = {DelayerApplication.class})
@Slf4j
class HighPriorityBatchServiceInMemoryTest {


    private HighPriorityBatchService highPriorityService;

    @Autowired
    PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl paperDeliveryDriverUsedCapacities;

    @Autowired
    PaperDeliveryDriverCapacitiesInMemoryDbImpl paperDeliveryDriverCapacities;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryUtils paperDeliveryUtils;

    private List<String> paperDeliveryTupleInMemory;


    @BeforeAll
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl(objectMapper);
        highPriorityService = new HighPriorityBatchServiceImpl(paperDeliveryDriverUsedCapacities, paperDeliveryDriverCapacities, paperDeliveryHighPriority, paperDeliveryUtils);
        paperDeliveryTupleInMemory = dao.retrievePartition();
    }

    private Stream<String> paperDeliveryTupleProvider() {
        return paperDeliveryTupleInMemory.stream();
    }

    @ParameterizedTest
    @MethodSource("paperDeliveryTupleProvider")
    void parameterizedJobRunTest(String tuple) {
        highPriorityService.initHighPriorityBatch(tuple, new HashMap<>(), Instant.now()).block();
        verify(tuple);
    }

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


        Assertions.assertEquals(0, highPriorityList.size());
        Assertions.assertEquals(1000, usedProvinceCapacity);
        Assertions.assertEquals(500, finalUsedCapCapacity.get("58010"));
        Assertions.assertEquals(500, finalUsedCapCapacity.get("58100"));
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(1, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(2, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(3, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(4, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(5, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(136, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(6, ChronoUnit.DAYS)).size());
    }


    private void verify(String tuple) {
        Instant deliveryWeek = paperDeliveryUtils.calculateDeliveryWeek(Instant.now());

        String[] tupleSplit = tuple.split("~");
        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        Integer usedProvinceCapacity = paperDeliveryDriverUsedCapacities.get(tupleSplit[0], tupleSplit[1], deliveryWeek).block();
        List<String> capList = List.of("58010", "58100", "18100", "37028", "20080", "65010", "71029", "22031", "61010", "90010");
        Map<String, Integer> finalUsedCapCapacity = new HashMap<>();
        capList.forEach(cap -> {
            Integer usedCapacity = paperDeliveryDriverUsedCapacities.get(tupleSplit[0], cap, deliveryWeek).block();
            finalUsedCapCapacity.put(cap, usedCapacity);
        });


        if (tuple.equalsIgnoreCase("3~GR")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(1000, usedProvinceCapacity);
            Assertions.assertEquals(500, finalUsedCapCapacity.get("58010"));
            Assertions.assertEquals(500, finalUsedCapCapacity.get("58100"));
        }

        if (tuple.equalsIgnoreCase("4~IM")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, usedProvinceCapacity);
            Assertions.assertEquals(400, finalUsedCapCapacity.get("18100"));
        }

        if (tuple.equalsIgnoreCase("5~VR")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, usedProvinceCapacity);
            Assertions.assertEquals(100, finalUsedCapCapacity.get("37028"));
        }

        if (tuple.equalsIgnoreCase("4~MI")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, usedProvinceCapacity);
            Assertions.assertEquals(500, finalUsedCapCapacity.get("20080"));
        }

        if (tuple.equalsIgnoreCase("1~PE")) {
            Assertions.assertEquals(200, highPriorityList.size());
            Assertions.assertEquals(300, usedProvinceCapacity);
            Assertions.assertEquals(300, finalUsedCapCapacity.get("65010"));
        }

        if (tuple.equalsIgnoreCase("6~FG")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, usedProvinceCapacity);
            Assertions.assertEquals(100, finalUsedCapCapacity.get("71029"));
        }

        if (tuple.equalsIgnoreCase("3~CO")) {
            Assertions.assertEquals(5, highPriorityList.size());
            Assertions.assertEquals(500, usedProvinceCapacity);
            Assertions.assertEquals(500, finalUsedCapCapacity.get("22031"));
            Assertions.assertEquals(5, highPriorityList.stream().filter(delivery -> delivery.getUnifiedDeliveryDriverGeoKey().equals("3~CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-31T")).count());
            Assertions.assertEquals(0, highPriorityList.stream().filter(delivery -> delivery.getUnifiedDeliveryDriverGeoKey().equals("3~CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-24T")).count());
        }


        if (tuple.equalsIgnoreCase("1~PU")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, usedProvinceCapacity);
            Assertions.assertEquals(500, finalUsedCapCapacity.get("61010"));
        }

        if (tuple.equalsIgnoreCase("5~PA")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, usedProvinceCapacity);
            Assertions.assertEquals(1000, finalUsedCapCapacity.get("90010"));
        }
    }
}

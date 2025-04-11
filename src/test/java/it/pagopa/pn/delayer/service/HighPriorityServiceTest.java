package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.DelayerApplication;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.inmemory.DeliveryDriverProvincePartitionInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryHighPriorityInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryReadyToSendInMemoryDbImpl;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestPropertySource(properties = {
        "pn.delayer.storage.impl=INMEMORY",
        "spring.application.name=PN-DELAYER-MS-BE",
        "pn.delayer.delivery-date-day-of-week=1",
        "pn.delayer.high-priority-query-limit=1000",
        "pn.delayer.delivery-date-interval=1d"})
@SpringJUnitConfig(classes = {DelayerApplication.class})
@Slf4j
@Execution(ExecutionMode.CONCURRENT)
class HighPriorityServiceTest {

    @Autowired
    HighPriorityServiceImpl highPriorityService;

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl paperDeliveryDriverCapacitiesDispatched;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryUtils paperDeliveryUtils;

    private static List<String> paperDeliveryTupleInMemory;

    @BeforeAll
    static void setUp() throws IOException {
        DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl();
        paperDeliveryTupleInMemory = dao.retrievePartition();
    }

    private static Stream<String> paperDeliveryTupleProvider() {
        return paperDeliveryTupleInMemory.stream();
    }

    @ParameterizedTest
    @MethodSource("paperDeliveryTupleProvider")
    void parameterizedJobRunTest(String tuple) {
        highPriorityService.initHighPriorityBatch(tuple).block();
        verify(tuple);
    }

    @Test
    void testInitHighPriorityBatch() {
        highPriorityService.initHighPriorityBatch("3##GR").block();
        Instant deliveryWeek = paperDeliveryUtils.calculateNextWeek(Instant.now());

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get("3##GR");
        Integer dispatchedProvinceCapacity = paperDeliveryDriverCapacitiesDispatched.get("3", "GR", deliveryWeek).block();
        List<String> capList = List.of("58010", "58100");
        Map<String, Integer> finalDispatchedCapCapacity = new HashMap<>();
        capList.forEach(cap -> {
            Integer dispatchedCapacity = paperDeliveryDriverCapacitiesDispatched.get("3", cap, deliveryWeek).block();
            finalDispatchedCapCapacity.put(cap, dispatchedCapacity);
        });


        Assertions.assertEquals(0, highPriorityList.size());
        Assertions.assertEquals(1000, dispatchedProvinceCapacity);
        Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58010"));
        Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58100"));
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(1, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(2, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(3, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(4, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(144, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(5, ChronoUnit.DAYS)).size());
        Assertions.assertEquals(136, paperDeliveryReadyToSend.getByDeliveryDate(deliveryWeek.plus(6, ChronoUnit.DAYS)).size());
    }


    private void verify(String tuple) {
        Instant deliveryWeek = paperDeliveryUtils.calculateNextWeek(Instant.now());

        String[] tupleSplit = tuple.split("##");
        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        Integer dispatchedProvinceCapacity = paperDeliveryDriverCapacitiesDispatched.get(tupleSplit[0], tupleSplit[1], deliveryWeek).block();
        List<String> capList = List.of("58010", "58100", "18100", "37028", "20080", "65010", "71029", "22031", "61010", "90010");
        Map<String, Integer> finalDispatchedCapCapacity = new HashMap<>();
        capList.forEach(cap -> {
            Integer dispatchedCapacity = paperDeliveryDriverCapacitiesDispatched.get(tupleSplit[0], cap, deliveryWeek).block();
            finalDispatchedCapCapacity.put(cap, dispatchedCapacity);
        });


        if (tuple.equalsIgnoreCase("3##GR")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(1000, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58010"));
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58100"));
        }

        if (tuple.equalsIgnoreCase("4##IM")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, dispatchedProvinceCapacity);
            Assertions.assertEquals(400, finalDispatchedCapCapacity.get("18100"));
        }

        if (tuple.equalsIgnoreCase("5##VR")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, dispatchedProvinceCapacity);
            Assertions.assertEquals(100, finalDispatchedCapCapacity.get("37028"));
        }

        if (tuple.equalsIgnoreCase("4##MI")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("20080"));
        }

        if (tuple.equalsIgnoreCase("1##PE")) {
            Assertions.assertEquals(200, highPriorityList.size());
            Assertions.assertEquals(300, dispatchedProvinceCapacity);
            Assertions.assertEquals(300, finalDispatchedCapCapacity.get("65010"));
        }

        if (tuple.equalsIgnoreCase("6##FG")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, dispatchedProvinceCapacity);
            Assertions.assertEquals(100, finalDispatchedCapCapacity.get("71029"));
        }

        if (tuple.equalsIgnoreCase("3##CO")) {
            Assertions.assertEquals(5, highPriorityList.size());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("22031"));
            Assertions.assertEquals(5, highPriorityList.stream().filter(delivery -> delivery.getDeliveryDriverIdGeoKey().equals("3##CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-31T")).count());
            Assertions.assertEquals(0, highPriorityList.stream().filter(delivery -> delivery.getDeliveryDriverIdGeoKey().equals("3##CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-24T")).count());
        }


        if (tuple.equalsIgnoreCase("1##PU")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("61010"));
        }

        if (tuple.equalsIgnoreCase("5##PA")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, dispatchedProvinceCapacity);
            Assertions.assertEquals(1000, finalDispatchedCapCapacity.get("90010"));
        }
    }
}

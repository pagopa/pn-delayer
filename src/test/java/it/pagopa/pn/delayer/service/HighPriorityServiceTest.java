package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.DelayerApplication;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.middleware.dao.inmemory.DeliveryDriverProvincePartitionInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryHighPriorityInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.inmemory.PaperDeliveryReadyToSendInMemoryDbImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestPropertySource("classpath:application-test.properties")
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

    private static List<String> paperDeliveryTupleInMemory;

    @BeforeAll
    static void setUp() throws IOException {
        DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl();
        paperDeliveryTupleInMemory = List.of("1##PU");//dao.retrievePartition();
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

    private void verify( String tuple) {
        String[] tupleSplit = tuple.split("##");
        List<PaperDeliveryReadyToSend> readyToSendList = paperDeliveryReadyToSend.getByDeliveryDate(Instant.now().plus(14, ChronoUnit.DAYS).toString());
        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        Integer dispatchedProvinceCapacity = paperDeliveryDriverCapacitiesDispatched.get(tupleSplit[0], tupleSplit[1]).block();
        List<String> capList = List.of("58010", "58100", "18100", "37028", "20080", "65010", "71029", "22031", "61010", "90010");
        Map<String, Integer> finalDispatchedCapCapacity = new HashMap<>();
        capList.forEach(cap -> {
                    Integer dispatchedCapacity = paperDeliveryDriverCapacitiesDispatched.get(tupleSplit[0], cap).block();
                    finalDispatchedCapCapacity.put(cap, dispatchedCapacity);
                });
        Instant deliveryWeek = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.of(1))).atStartOfDay().toInstant(ZoneOffset.UTC);
        if (tuple.equalsIgnoreCase("3##GR")) {

            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(1000, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR")).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR") && delivery.getDeliveryDate().equals(deliveryWeek)).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR") && delivery.getDeliveryDate().equals(deliveryWeek.plus(1, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR") && delivery.getDeliveryDate().equals(deliveryWeek.plus(2, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR")  && delivery.getDeliveryDate().equals(deliveryWeek.plus(3, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR") && delivery.getDeliveryDate().equals(deliveryWeek.plus(4, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(144, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR")  && delivery.getDeliveryDate().equals(deliveryWeek.plus(5, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(136, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("GR")  && delivery.getDeliveryDate().equals(deliveryWeek.plus(6, ChronoUnit.DAYS))).count());
            Assertions.assertEquals(1000, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58010"));
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("58100"));
        }

        if (tuple.equalsIgnoreCase("4##IM")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("4")
                    && delivery.getProvince().equalsIgnoreCase("IM")).count());
            Assertions.assertEquals(400, dispatchedProvinceCapacity);
            Assertions.assertEquals(400, finalDispatchedCapCapacity.get("18100"));
        }

        if (tuple.equalsIgnoreCase("5##VR")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("5")
                    && delivery.getProvince().equalsIgnoreCase("VR")).count());
            Assertions.assertEquals(100, dispatchedProvinceCapacity);
            Assertions.assertEquals(100, finalDispatchedCapCapacity.get("37028"));
        }

        if (tuple.equalsIgnoreCase("4##MI")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("4")
                    && delivery.getProvince().equalsIgnoreCase("MI")).count());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("20080"));
        }

        if (tuple.equalsIgnoreCase("1##PE")) {
            Assertions.assertEquals(200, highPriorityList.size());
            Assertions.assertEquals(300, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("1")
                    && delivery.getProvince().equalsIgnoreCase("PE")).count());
            Assertions.assertEquals(300, dispatchedProvinceCapacity);
            Assertions.assertEquals(300, finalDispatchedCapCapacity.get("65010"));
        }

        if (tuple.equalsIgnoreCase("6##FG")) {
            Assertions.assertEquals(400, highPriorityList.size());
            Assertions.assertEquals(100, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("6")
                    && delivery.getProvince().equalsIgnoreCase("FG")).count());
            Assertions.assertEquals(100, dispatchedProvinceCapacity);
            Assertions.assertEquals(100, finalDispatchedCapCapacity.get("71029"));
        }

        if (tuple.equalsIgnoreCase("3##CO")) {
            Assertions.assertEquals(5, highPriorityList.size());
            Assertions.assertEquals(500, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("3")
                    && delivery.getProvince().equalsIgnoreCase("CO")).count());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("22031"));
            Assertions.assertEquals(5, highPriorityList.stream().filter(delivery -> delivery.getPk().equals("3##CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-31T")).count());
            Assertions.assertEquals(0, highPriorityList.stream().filter(delivery -> delivery.getPk().equals("3##CO")
                    && delivery.getCreatedAt().toString().contains("2025-03-24T")).count());
        }


        if (tuple.equalsIgnoreCase("1##PU")) {
            Assertions.assertEquals(0, highPriorityList.size());
            Assertions.assertEquals(500, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("1")
                    && delivery.getProvince().equalsIgnoreCase("PU")).count());
            Assertions.assertEquals(500, dispatchedProvinceCapacity);
            Assertions.assertEquals(500, finalDispatchedCapCapacity.get("61010"));
        }

        if (tuple.equalsIgnoreCase("5##PA")) {
            Assertions.assertEquals(100, highPriorityList.size());
            Assertions.assertEquals(400, readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals("5")
                    && delivery.getProvince().equalsIgnoreCase("PA")).count());
            Assertions.assertEquals(400, dispatchedProvinceCapacity);
            Assertions.assertEquals(1000, finalDispatchedCapCapacity.get("90010"));
        }
    }
}

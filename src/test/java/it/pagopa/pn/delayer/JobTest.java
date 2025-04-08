package it.pagopa.pn.delayer;

import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.middleware.dao.inmemory.*;
import it.pagopa.pn.delayer.service.HighPriorityServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

@TestPropertySource("classpath:application-test.properties")
@SpringJUnitConfig(classes = {DelayerApplication.class})
@Slf4j
@Execution(ExecutionMode.CONCURRENT)
class JobTest {

    @Autowired
    HighPriorityServiceImpl highPriorityService;

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl paperDeliveryDriverCapacitiesDispatched;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryDriverCapacitiesInMemoryDbImpl paperDeliveryDriverCapacities;

    private static List<String> paperDeliveryTupleInMemory;
    private static final String REPORT_FILE = "src/test/resources/test_report.csv";
    private static Path reportFilePath;
    private static final List<TestReport> testReports = new ArrayList<>();
    private static Instant startTime;

    private final String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";

    @BeforeAll
    static void setUp() throws IOException {
        startTime = Instant.now();
        DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl();
       paperDeliveryTupleInMemory = dao.retrievePartition();
        reportFilePath = Paths.get(REPORT_FILE);
        Files.deleteIfExists(reportFilePath);
        List<String> header = List.of("deliveryDriverId,province,paperDeliveryRequest,provinceCapacity,startedDispatchedProvinceCapacity,finalDispatchedProvinceCapacity,capCapacity,startedDispatchedCapCapacity,finalDispatchedCapCapacity,paperDeliveryReadyToSend,paperDeliveryExcess,executionTime");
        Files.write(reportFilePath, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @AfterAll
    static void closeReportFileNio() throws IOException {
        Instant endTime = Instant.now();
        testReports.sort(Comparator.comparing(TestReport::getPaperDeliveryRequest).reversed());
        Files.write(reportFilePath, testReports.stream().map(TestReport::toString).toList(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        Duration duration = Duration.between(startTime, endTime);
        String reportLine = "Tempo totale di esecuzione: " + duration.toMillis() + "ms";
        Files.write(reportFilePath, List.of(reportLine), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static Stream<String> paperDeliveryTupleProvider() {
        return paperDeliveryTupleInMemory.stream();
    }

    @ParameterizedTest
    @MethodSource("paperDeliveryTupleProvider")
    void parameterizedJobRunTest(String tuple) {
        TestReport testReport = prepareTestReport(tuple);
        Instant start = Instant.now();
        highPriorityService.initHighPriorityBatch(tuple).block();
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        verifyAndCloseTestReport(testReport, duration.toMillis(), tuple);
    }

    private TestReport prepareTestReport(String tuple) {
        TestReport testReport = new TestReport(tuple);

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        testReport.setPaperDeliveryRequest(highPriorityList.size());

        Integer provinceCapacity = paperDeliveryDriverCapacities.getPaperDeliveryDriverCapacities(tenderId, testReport.getDeliveryDriverId(), testReport.getProvince()).block();
        testReport.setProvinceCapacity(provinceCapacity);
        testReport.setStartedDispatchedProvinceCapacity(paperDeliveryDriverCapacitiesDispatched.get(testReport.getDeliveryDriverId(), testReport.getProvince()).block());

        HashMap<String, Integer> capDispatchedCapacity = new HashMap<>();
        HashMap<String, Integer> capCapacity = new HashMap<>();
        highPriorityList.stream()
                .map(PaperDeliveryHighPriority::getCap)
                .distinct()
                .toList()
                .forEach(cap -> {
                    capDispatchedCapacity.put(cap, paperDeliveryDriverCapacitiesDispatched.get(testReport.getDeliveryDriverId(), cap).block());
                    capCapacity.put(cap,  paperDeliveryDriverCapacities.getPaperDeliveryDriverCapacities(tenderId, testReport.getDeliveryDriverId(), cap).block());
                });
        testReport.setCapCapacity(capCapacity);
        testReport.setStartedDispatchedCapCapacity(capDispatchedCapacity);
        return testReport;
    }

    private void verifyAndCloseTestReport(TestReport testReport, Long duration, String tuple) {
        testReport.setExecutionTime(duration + "ms");

        List<PaperDeliveryReadyToSend> readyToSendList = paperDeliveryReadyToSend.getByDeliveryDate(Instant.now().plus(14, java.time.temporal.ChronoUnit.DAYS).toString());
        testReport.setPaperDeliveryReadyToSend(readyToSendList.stream().filter(delivery -> delivery.getDeliveryDriverId().equals(testReport.getDeliveryDriverId())
                && delivery.getProvince().equalsIgnoreCase(testReport.getProvince())).count());

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        testReport.setPaperDeliveryExcess(highPriorityList.size());

        Integer dispatchedProvinceCapacity = paperDeliveryDriverCapacitiesDispatched.get(testReport.getDeliveryDriverId(), testReport.getProvince()).block();
        testReport.setFinalDispatchedProvinceCapacity(dispatchedProvinceCapacity);

        Map<String, Integer> finalDispatchedCapCapacity = new HashMap<>();
        testReport.getStartedDispatchedCapCapacity().keySet()
                .forEach(cap -> {
                    finalDispatchedCapCapacity.put(cap, paperDeliveryDriverCapacitiesDispatched.get(testReport.getDeliveryDriverId(), cap).block());
                });
        testReport.setFinalDispatchedCapCapacity(finalDispatchedCapCapacity);
        testReports.add(testReport);
    }
}

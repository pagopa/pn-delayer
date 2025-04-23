package it.pagopa.pn.delayer;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.inmemory.*;
import it.pagopa.pn.delayer.service.HighPriorityBatchServiceImpl;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
@Execution(ExecutionMode.CONCURRENT)
@Disabled("This test should only be run locally")
class JobTest {

    @Autowired
    HighPriorityBatchServiceImpl highPriorityService;

    @Autowired
    PaperDeliveryDriverUsedCapacitiesInMemoryDbImpl paperDeliveryDriverUsedCapacities;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryDriverCapacitiesInMemoryDbImpl paperDeliveryDriverCapacities;

    @Autowired
    PaperDeliveryUtils paperDeliveryUtils;

    private static List<String> paperDeliveryTupleInMemory;
    private static final String REPORT_FILE = "src/test/resources/test_report.csv";
    private static Path reportFilePath;
    private static final List<TestReport> testReports = new ArrayList<>();
    private static Instant startTime;

    private final String tenderId = "c0d82f6e-ee85-4e27-97df-bef27c5c5377";

    @BeforeAll
    static void setUp() throws IOException {
        startTime = Instant.now();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        DeliveryDriverProvincePartitionInMemoryDbImpl dao = new DeliveryDriverProvincePartitionInMemoryDbImpl(objectMapper);
        paperDeliveryTupleInMemory = dao.retrievePartition();
        reportFilePath = Paths.get(REPORT_FILE);
        Files.deleteIfExists(reportFilePath);
        List<String> header = List.of("unifiedDeliveryDriver,province,paperDeliveryRequest,provinceCapacity,startedUsedProvinceCapacity,finalUsedProvinceCapacity,capCapacity,startedUsedCapCapacity,finalUsedCapCapacity,paperDeliveryExcess,executionTime");
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
        Instant now = Instant.now();
        Instant deliveryDate = paperDeliveryUtils.calculateDeliveryWeek(now);
        TestReport testReport = prepareTestReport(tuple,deliveryDate);
        Instant start = Instant.now();

        highPriorityService.initHighPriorityBatch(tuple, new HashMap<>(), now).block();
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        verifyAndCloseTestReport(testReport, duration.toMillis(), tuple);
    }

    private TestReport prepareTestReport(String tuple, Instant now) {
        TestReport testReport = new TestReport(tuple);

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        testReport.setPaperDeliveryRequest(highPriorityList.size());

        Integer provinceCapacity = paperDeliveryDriverCapacities.getPaperDeliveryDriverCapacities(tenderId, testReport.getUnifiedDeliveryDriver(), testReport.getProvince(), Instant.now()).block();
        testReport.setProvinceCapacity(provinceCapacity);
        testReport.setStartedUsedProvinceCapacity(paperDeliveryDriverUsedCapacities.get(testReport.getUnifiedDeliveryDriver(), testReport.getProvince(), now).block());

        HashMap<String, Integer> capUsedCapacity = new HashMap<>();
        HashMap<String, Integer> capCapacity = new HashMap<>();
        highPriorityList.stream()
                .map(PaperDeliveryHighPriority::getCap)
                .distinct()
                .toList()
                .forEach(cap -> {
                    capUsedCapacity.put(cap, paperDeliveryDriverUsedCapacities.get(testReport.getUnifiedDeliveryDriver(), cap, now).block());
                    capCapacity.put(cap,  paperDeliveryDriverCapacities.getPaperDeliveryDriverCapacities(tenderId, testReport.getUnifiedDeliveryDriver(), cap, Instant.now()).block());
                });
        testReport.setCapCapacity(capCapacity);
        testReport.setStartedUsedCapCapacity(capUsedCapacity);
        return testReport;
    }

    private void verifyAndCloseTestReport(TestReport testReport, Long duration, String tuple) {
        Instant now = paperDeliveryUtils.calculateDeliveryWeek(Instant.now());

        testReport.setExecutionTime(duration + "ms");

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.get(tuple);
        testReport.setPaperDeliveryExcess(highPriorityList.size());

        Integer usedProvinceCapacity = paperDeliveryDriverUsedCapacities.get(testReport.getUnifiedDeliveryDriver(), testReport.getProvince(), now).block();
        testReport.setFinalUsedProvinceCapacity(usedProvinceCapacity);

        Map<String, Integer> finalUsedCapCapacity = new HashMap<>();
        testReport.getStartedUsedCapCapacity().keySet()
                .forEach(cap -> finalUsedCapCapacity.put(cap, paperDeliveryDriverUsedCapacities.get(testReport.getUnifiedDeliveryDriver(), cap, now).block()));
        testReport.setFinalUsedCapCapacity(finalUsedCapCapacity);
        testReports.add(testReport);
    }
}

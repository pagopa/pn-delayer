/*
package it.pagopa.pn.delayer;

import it.pagopa.pn.delayer.config.PnDelayerConfig;
import it.pagopa.pn.delayer.middleware.dao.DeliveryDriverProvincePartitionDAO;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacities;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryDriverCapacitiesDispatched;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.middleware.dao.impl.PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.impl.PaperDeliveryDriverCapacitiesInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.impl.PaperDeliveryHighPriorityInMemoryDbImpl;
import it.pagopa.pn.delayer.middleware.dao.impl.PaperDeliveryReadyToSendInMemoryDbImpl;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@SpringBatchTest
@TestPropertySource("classpath:application-test.properties")
@SpringJUnitConfig(classes = {DelayerApplication.class})
@Sql("classpath:schema-h2-test.sql")
class JobTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    DeliveryDriverProvincePartitionDAO dao;

    @Autowired
    PaperDeliveryDriverCapacitiesDispatchedInMemoryDbImpl paperDeliveryDriverCapacitiesDispatched;

    @Autowired
    PaperDeliveryHighPriorityInMemoryDbImpl paperDeliveryHighPriority;

    @Autowired
    PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSend;

    @Autowired
    PaperDeliveryDriverCapacitiesInMemoryDbImpl paperDeliveryDriverCapacities;

    @Test
    void testJob(@Autowired Job job) throws Exception {
        jobLauncherTestUtils.setJob(job);
        List<String> paperDeliveryTupleInMemory = dao.retrievePartition();
        var pks = String.join(",", paperDeliveryTupleInMemory);
        JobParameters jobParameters = new JobParametersBuilder()
                .addJobParameter("pks", pks, String.class)
                .addJobParameter("createdAt", Instant.now().toString(), String.class)
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        Assertions.assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        Assertions.assertEquals(637, jobExecution.getStepExecutions().size());
        Assertions.assertEquals(637, jobExecution.getStepExecutions().stream().filter(stepExecution -> stepExecution.getExitStatus().getExitCode().equals("COMPLETED")).count());
        Assertions.assertEquals(0, jobExecution.getAllFailureExceptions().size());

        List<PaperDeliveryHighPriority> highPriorityList = paperDeliveryHighPriority.getAll();
        List<PaperDeliveryReadyToSend> readyToSendList = paperDeliveryReadyToSend.getByDeliveryDate(Instant.now().toString());
        Collection<PaperDeliveryDriverCapacitiesDispatched> dispatchedList = paperDeliveryDriverCapacitiesDispatched.getAll();
        Collection<PaperDeliveryDriverCapacities> capacitiesList = paperDeliveryDriverCapacities.getAll();

        //controllo eccedenze
        Assertions.assertEquals(1105, highPriorityList.size());
        Assertions.assertEquals(0, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("3##GR")).count());
        Assertions.assertEquals(0, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("4##MI")).count());
        Assertions.assertEquals(0, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("1##PU")).count());
        Assertions.assertEquals(0, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("5##PA")).count());
        Assertions.assertEquals(5, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("3##CO")).count());
        Assertions.assertEquals(400, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("6##FG")).count());
        Assertions.assertEquals(400, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("5##VR")).count());
        Assertions.assertEquals(100, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("4##IM")).count());
        Assertions.assertEquals(5, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("3##CO")
                && paperDeliveryHighPriority.getCreatedAt().equalsIgnoreCase("2025-03-31T13:09:19.972634Z")).count());
        Assertions.assertEquals(0, highPriorityList.stream().filter(paperDeliveryHighPriority -> paperDeliveryHighPriority.getPk().equals("3##CO")
                && paperDeliveryHighPriority.getCreatedAt().equalsIgnoreCase("2025-03-24T13:09:19.960638Z")).count());

        //controllo readyToSend
        Assertions.assertEquals(3900, readyToSendList.size());

        //controllo capacità utilizzata
        Assertions.assertEquals(19, dispatchedList.size());


    }
}
*/

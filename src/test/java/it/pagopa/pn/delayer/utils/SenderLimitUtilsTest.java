package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.SenderLimitMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SenderLimitUtilsTest {

    @InjectMocks
    private SenderLimitUtils senderLimitUtils;

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Test
    void retrieveAndEvaluateSenderLimit_handlesValidInput() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId = Map.of("key1", List.of(new PaperDelivery()));
        SenderLimitMaps senderLimitMaps = new SenderLimitMaps();
        Integer capacity = 100;
        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();

        when(paperDeliverySenderLimitDAO.retrieveUsedSendersLimit(anyList(), eq(deliveryWeek)))
                .thenReturn(Flux.empty());
        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryWeek)))
                .thenReturn(Flux.empty());

        StepVerifier.create(senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, senderLimitMaps, capacity, senderLimitJobPaperDeliveries))
                .verifyComplete();
    }

    @Test
    void retrieveAndEvaluateSenderLimit_handlesEmptyDeliveries() {
        LocalDate deliveryWeek = LocalDate.now();
        Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId = Map.of();
        SenderLimitMaps senderLimitMaps = new SenderLimitMaps();
        Integer capacity = 100;
        SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries = new SenderLimitJobPaperDeliveries();

        StepVerifier.create(senderLimitUtils.retrieveAndEvaluateSenderLimit(deliveryWeek, deliveriesGroupedByProductTypePaId, senderLimitMaps, capacity, senderLimitJobPaperDeliveries))
                .verifyComplete();
    }

    @Test
    void updateUsedSenderLimit_updatesCorrectly() {
        List<PaperDelivery> paperDeliveryList = List.of(new PaperDelivery());
        LocalDate deliveryDate = LocalDate.now();
        Map<String, Integer> senderLimitMap = Map.of("key1", 10);

        when(paperDeliverySenderLimitDAO.updateUsedSenderLimit(anyString(), anyInt(), eq(deliveryDate), anyInt()))
                .thenReturn(Mono.just(1));

        StepVerifier.create(senderLimitUtils.updateUsedSenderLimit(paperDeliveryList, deliveryDate, senderLimitMap))
                .expectNext(1)
                .verifyComplete();
    }

    @Test
    void updateUsedSenderLimit_handlesEmptyPaperDeliveryList() {
        List<PaperDelivery> paperDeliveryList = List.of();
        LocalDate deliveryDate = LocalDate.now();
        Map<String, Integer> senderLimitMap = Map.of();

        StepVerifier.create(senderLimitUtils.updateUsedSenderLimit(paperDeliveryList, deliveryDate, senderLimitMap))
                .expectNext(0)
                .verifyComplete();
    }
}

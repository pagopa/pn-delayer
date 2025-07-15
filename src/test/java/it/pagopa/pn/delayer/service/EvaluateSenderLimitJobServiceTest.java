package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliverySenderLimitDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliverySenderLimit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluateSenderLimitJobServiceTest {

    @Mock
    private PaperDeliverySenderLimitDAO paperDeliverySenderLimitDAO;

    @Mock
    private PnDelayerConfigs pnDelayerConfigs;

    @InjectMocks
    private EvaluateSenderLimitJobServiceImpl evaluateSenderLimitJobService;

    @Test
    void calculateSenderLimit_returnsCorrectSenderLimitMap() {
        LocalDate deliveryDate = LocalDate.now();
        Map<String, Integer> capacityMap = Map.of("ProductA", 100, "ProductB", 200);
        String province = "RM";
        List<String> paIdProductTypeTuples = List.of("PA1~ProductA", "PA2~ProductB", "PA2~ProductA");

        PaperDeliverySenderLimit limit1 = new PaperDeliverySenderLimit();
        limit1.setPk("PA1~ProductA~RM");
        limit1.setProductType("ProductA");
        limit1.setPercentageLimit(20);
        limit1.setDeliveryDate("2025-02-03");
        limit1.setProvince("RM");
        limit1.setPaId("PA1");

        PaperDeliverySenderLimit limit2 = new PaperDeliverySenderLimit();
        limit2.setPk("PA2~ProductA~RM");
        limit2.setProductType("ProductA");
        limit2.setPercentageLimit(50);
        limit2.setDeliveryDate("2025-02-03");
        limit2.setProvince("RM");
        limit2.setPaId("PA2");

        PaperDeliverySenderLimit limit3 = new PaperDeliverySenderLimit();
        limit3.setPk("PA2~ProductB~RM");
        limit3.setProductType("ProductB");
        limit3.setPercentageLimit(25);
        limit3.setDeliveryDate("2025-02-03");
        limit3.setProvince("RM");
        limit3.setPaId("PA2");

        when(paperDeliverySenderLimitDAO.retrieveSendersLimit(anyList(), eq(deliveryDate)))
                .thenReturn(Flux.just(limit1, limit2, limit3));

        StepVerifier.create(evaluateSenderLimitJobService.calculateSenderLimit(deliveryDate, capacityMap, province, paIdProductTypeTuples))
                .expectNextMatches(senderLimitMap ->
                        senderLimitMap.get("PA1~ProductA~RM") == 20 &&
                                senderLimitMap.get("PA2~ProductB~RM") == 50 &&
                        senderLimitMap.get("PA2~ProductA~RM") == 50)
                .verifyComplete();
    }
}

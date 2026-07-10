package it.pagopa.pn.delayer.service;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class EvaluateSenderPriorityJobServiceImpl implements EvaluateSenderPriorityJobService{

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PnDelayerConfigs pnDelayerConfigs;

    @Override
    public Mono<Void> startSenderPriorityJob(String senderPaId, LocalDate deliveryWeek) {

        String pk = deliveryWeek + "~EVALUATE_SENDER_LIMIT";
        List<Instant> originalOrderedDates = new ArrayList<>();
        NavigableMap<Integer, List<PaperDelivery>> deliveriesByPriority = new TreeMap<>(Comparator.reverseOrder());
        String senderPaIdSkPrefix = senderPaId + "~";

        return retrievePendingDeliveriesBySenderWeek(pk, senderPaIdSkPrefix, null)
                .expand(page -> {
                    if (CollectionUtils.isEmpty(page.lastEvaluatedKey())) {
                        return Mono.empty();
                    }
                    return retrievePendingDeliveriesBySenderWeek(pk, senderPaIdSkPrefix, page.lastEvaluatedKey());
                })
                .map(Page::items)
                .flatMapIterable(items -> items)
                .doOnNext(delivery -> {
                    String sentAt = delivery.getSenderPaIdOriginalSentAt().split("~")[1];
                    originalOrderedDates.add(Instant.parse(sentAt));
                    deliveriesByPriority.computeIfAbsent(delivery.getSenderPriority(), ignored -> new ArrayList<>()).add(delivery);
                })
                .then(Mono.defer(() -> {
                    if (originalOrderedDates.isEmpty()) {
                        return Mono.empty();
                    }
                    List<PaperDelivery> reorderedDeliveries = buildReorderedDeliveries(originalOrderedDates, deliveriesByPriority);
                    return writeReorderedDeliveries(reorderedDeliveries);
                }));
    }

    private Mono<Page<PaperDelivery>> retrievePendingDeliveriesBySenderWeek(String pk, String skPrefix, Map<String, AttributeValue> lastEvaluatedKey) {
        return paperDeliveryUtils.retrievePaperDeliveriesToReorder(WorkflowStepEnum.EVALUATE_SENDER_LIMIT, LocalDate.parse(pk.split("~")[0]), skPrefix, lastEvaluatedKey, pnDelayerConfigs.getDao().getPaperDeliveryQueryLimit(), PaperDelivery.PK_SENDERPAID_ORIGINALSENTAT_INDEX);
    }

    private List<PaperDelivery> buildReorderedDeliveries(List<Instant> originalOrderedDates, NavigableMap<Integer, List<PaperDelivery>> deliveriesByPriority) {
        List<PaperDelivery> result = new ArrayList<>(originalOrderedDates.size());
        int index = 0;
        for (Map.Entry<Integer, List<PaperDelivery>> entry : deliveriesByPriority.entrySet()) {
            List<PaperDelivery> deliveriesForPriority = entry.getValue();
            for (PaperDelivery delivery : deliveriesForPriority) {
                Instant effectiveSortEpochMillis = originalOrderedDates.get(index++);
                PaperDelivery reordered = copyWithEffectiveSort(delivery, effectiveSortEpochMillis);
                result.add(reordered);
            }
        }

        return result;
    }

    private PaperDelivery copyWithEffectiveSort(PaperDelivery delivery, Instant effectiveSortEpochMillis) {
        PaperDelivery copy = new PaperDelivery(delivery, WorkflowStepEnum.EVALUATE_SENDER_LIMIT, LocalDate.parse(delivery.getDeliveryDate()), delivery.getPreviousStep());
        copy.setOldSk(delivery.getSk());
        copy.setSk(buildFinalSortKey(delivery.getProvince(), effectiveSortEpochMillis, delivery.getRequestId()));
        copy.setVirtualNotificationSentAt(effectiveSortEpochMillis.toString());

        log.info("Reordering delivery with requestId {}: old sk was {}, new sk is {}", delivery.getRequestId(), copy.getOldSk(), copy.getSk());

        return copy;
    }

    private String buildFinalSortKey(String province, Instant effectiveSortEpochMillis, String shipmentId) {
        return province + "~" + effectiveSortEpochMillis + "~" + shipmentId;
    }

    private Mono<Void> writeReorderedDeliveries(List<PaperDelivery> reorderedDeliveries) {
        return Flux.fromIterable(reorderedDeliveries)
                .buffer(50) // 50 put + 50 delete = 100 transact actions
                .concatMap(paperDeliveryUtils::transactReorderedDeliveries)
                .then();
    }
}

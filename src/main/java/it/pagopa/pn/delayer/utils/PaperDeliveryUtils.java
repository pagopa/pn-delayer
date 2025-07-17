package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.DeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverRequest;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.util.function.Tuple2;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PaperDeliveryUtils {

    private final PnDelayerConfigs pnDelayerConfig;

    /**
     * This method filters the deliveries based on the remaining capacity and prepares them for the next step.
     * It also handles the case where there are more deliveries than the remaining capacity, moving excess deliveries to the next week.
     *
     * @param deliveries       List of PaperDelivery items to filter
     * @param capCapacities            Tuple containing total capacity and used capacity
     * @param deliveriesToSend List to add filtered deliveries that will be sent
     * @param toNextWeek       List to add deliveries that will be moved to next week
     * @return The number of deliveries that were filtered and prepared
     */
    public Integer filterAndPrepareDeliveries(List<PaperDelivery> deliveries, Tuple2<Integer, Integer> capCapacities, List<PaperDelivery> deliveriesToSend, List<PaperDelivery> toNextWeek, LocalDate deliveryWeek) {
        int remainingCapacity = Math.max(capCapacities.getT1() - capCapacities.getT2(), 0);
        List<PaperDelivery> filteredList = deliveries.stream().limit(remainingCapacity).toList();

        if (!CollectionUtils.isEmpty(filteredList)) {
            deliveriesToSend.addAll(mapItemForEvaluatePrintCapacityStep(filteredList, deliveryWeek));
        }

        if (deliveries.size() > filteredList.size()) {
            toNextWeek.addAll(deliveries.subList(filteredList.size(), deliveries.size()));
        }

        return filteredList.size();
    }

    /**
     * Metodo che raggruppa i CAP di uno stesso chunk di una stessa provincia
     *
     * @param paperDeliveries chunk di righe recuperate a DB di una stessa provincia
     * @return una mappa con chiave CAP e valore lista di righe aventi lo stesso CAP della chiave
     */
    public Map<String, List<PaperDelivery>> groupDeliveryOnCapAndOrderOnCreatedAt(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream()
                .collect(Collectors.groupingBy(
                        PaperDelivery::getCap,
                        Collectors.toList()));
    }

    public List<PaperDelivery> mapItemForEvaluatePrintCapacityStep(List<PaperDelivery> items, LocalDate deliveryWeek) {
        items.forEach(paperDelivery -> {
                    paperDelivery.setPk(deliveryWeek + "~" + WorkflowStepEnum.EVALUATE_PRINT_CAPACITY);
                    paperDelivery.setDeliveryDate(deliveryWeek.toString());
                    paperDelivery.setSk(String.join("~", String.valueOf(paperDelivery.getPriority()), paperDelivery.getRequestId()));
                });
        return items;
    }

    /**
     * This method calculates the start day of the delivery week based on the execution batch start date.
     */
    public LocalDate calculateDeliveryWeek(Instant startExcutionBatch) {
        LocalDate startDate = startExcutionBatch.atZone(ZoneOffset.UTC).toLocalDate();
        return startDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
    }

    /**
     * This method calculates the start day of the nextWeek week based on the deliveryWeek.
     */
    public LocalDate calculateNextWeek(LocalDate deliveryWeek) {
        return deliveryWeek.plusWeeks(1);
    }

    public List<PaperDelivery> toNextWeek(List<PaperDelivery> deliveries, LocalDate deliveryWeek) {
        deliveries.forEach(paperDelivery -> {
            paperDelivery.setPk(calculateNextWeek(deliveryWeek) + "~" + WorkflowStepEnum.EVALUATE_SENDER_LIMIT);
            paperDelivery.setSk(paperDelivery.getProvince() + "~" + retrieveDateForSk(paperDelivery) + "~" + paperDelivery.getRequestId());
        });
        return deliveries;
    }

    private String retrieveDateForSk(PaperDelivery paperDelivery) {
        return paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1 ?
                paperDelivery.getPrepareRequestDate() : paperDelivery.getNotificationSentAt();
    }

    public List<PaperDelivery> buildtoDriverCapacityEvaluation(List<PaperDelivery> deliveries, String unifiedDeliveryDriver, String tenderId, Map<Integer, List<String>> priorityMap) {
        return deliveries.stream()
                .map(paperDelivery -> {
                    String key = "PRODUCT_" + paperDelivery.getProductType() + ".ATTEMPT_" + paperDelivery.getAttempt();
                    Integer priority = priorityMap.entrySet().stream()
                            .filter(entry -> entry.getValue().contains(key))
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .orElse(3);

                    PaperDelivery toDriverCapacityEvaluation = new PaperDelivery();
                    toDriverCapacityEvaluation.setPk(PaperDelivery.buildPk(WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, paperDelivery.getDeliveryDate()));
                    toDriverCapacityEvaluation.setSk(PaperDelivery.buildSortKey(paperDelivery.getIun(), paperDelivery.getRecipientId()));
                    toDriverCapacityEvaluation.setCap(paperDelivery.getCap());
                    toDriverCapacityEvaluation.setProductType(paperDelivery.getProductType());
                    toDriverCapacityEvaluation.setUnifiedDeliveryDriver(unifiedDeliveryDriver);

                    toDriverCapacityEvaluation.setTenderId(tenderId);
                    toDriverCapacityEvaluation.setPriority(priority);
                    toDriverCapacityEvaluation.setAttempt(paperDelivery.getAttempt());
                    toDriverCapacityEvaluation.setRecipientId(paperDelivery.getRecipientId());
                    toDriverCapacityEvaluation.setIun(paperDelivery.getIun());
                    toDriverCapacityEvaluation.setCreatedAt(paperDelivery.getCreatedAt());
                    toDriverCapacityEvaluation.setRequestId(paperDelivery.getRequestId());
                    toDriverCapacityEvaluation.setNotificationSentAt(paperDelivery.getNotificationSentAt());
                    toDriverCapacityEvaluation.setPrepareRequestDate(paperDelivery.getPrepareRequestDate());
                    toDriverCapacityEvaluation.setSenderPaId(paperDelivery.getSenderPaId());
                    toDriverCapacityEvaluation.setProvince(paperDelivery.getProvince());
                    toDriverCapacityEvaluation.setDeliveryDate(paperDelivery.getDeliveryDate());
                    return toDriverCapacityEvaluation;
                })
                .collect(Collectors.toList());
    }

    public PaperChannelDeliveryDriverRequest constructPaperChannelTenderApiPayload(List<DeliveryDriverRequest> deliveryDriverRequests, String tenderId) {
        return new PaperChannelDeliveryDriverRequest(
                deliveryDriverRequests,
                tenderId,
                "GET_UNIFIED_DELIVERY_DRIVERS"
        );
    }

    public List<PaperDelivery> assignUnifiedDeliveryDriverAndBuildNewStepEntities(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses, Map<String, List<PaperDelivery>> groupedByCapProductType, String tenderId, Map<Integer, List<String>> priorityMap) {
        Map<String, String> map = groupByGeoKeyAndProduct(paperChannelDeliveryDriverResponses);
        return new ArrayList<>(groupedByCapProductType.entrySet().stream()
                .filter(stringListEntry -> map.containsKey(stringListEntry.getKey()))
                .flatMap(stringListEntry -> {
                    String unifiedDeliveryDriver = map.get(stringListEntry.getKey());
                    List<PaperDelivery> paperDeliveriesChunk = stringListEntry.getValue();
                    return buildtoDriverCapacityEvaluation(paperDeliveriesChunk, unifiedDeliveryDriver, tenderId, priorityMap).stream();
                })
                .toList());
    }

    public Map<String, String> groupByGeoKeyAndProduct(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses) {
        return paperChannelDeliveryDriverResponses.stream()
                .collect(Collectors.toMap(
                        item -> item.getGeoKey() + "~" + item.getProduct(),
                        PaperChannelDeliveryDriverResponse::getUnifiedDeliveryDriver
                ));
    }
}

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.PaperChannelDeliveryDriverResponse;
import it.pagopa.pn.delayer.model.SenderLimitJobPaperDeliveries;
import it.pagopa.pn.delayer.model.WorkflowStepEnum;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PnDelayerUtils {

    private final PnDelayerConfigs pnDelayerConfig;

    /**
     * This method calculates the start day of the delivery week based on the execution batch start date.
     */
    public LocalDate calculateDeliveryWeek(Instant startExcutionBatch) {
        LocalDate startDate = startExcutionBatch.atZone(ZoneOffset.UTC).toLocalDate();
        return startDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
    }

    public Map<String, List<PaperDelivery>> groupByCap(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream().collect(Collectors.groupingBy(PaperDelivery::getCap, Collectors.toList()));
    }

    public Map<String, List<PaperDelivery>> groupByPaIdProductTypeProvince(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream()
                .collect(Collectors.groupingBy(paperDelivery -> paperDelivery.getSenderPaId() + "~" + paperDelivery.getProductType() + "~" + paperDelivery.getProvince(),
                        Collectors.toList()));
    }

    public Map<String, Long> groupByPaIdProductTypeProvinceAndCount(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream()
                .collect(Collectors.groupingBy(
                        paperDelivery -> paperDelivery.getSenderPaId() + "~" + paperDelivery.getProductType() + "~" + paperDelivery.getProvince(),
                        Collectors.counting()));
    }

    public Map<String, String> groupByGeoKeyAndProduct(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses) {
        return paperChannelDeliveryDriverResponses.stream()
                .collect(Collectors.toMap(item -> item.getGeoKey() + "~" + item.getProduct(), PaperChannelDeliveryDriverResponse::getUnifiedDeliveryDriver, (x, y) -> x));
    }

    public Map<String, List<PaperDelivery>> groupByCapAndProductType(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream().collect(Collectors.groupingBy(paperDelivery -> paperDelivery.getCap() + "~" + paperDelivery.getProductType()));
    }

    public List<PaperDelivery> mapItemForResidualCapacityStep(List<PaperDelivery> paperDeliveries, LocalDate deliveryWeek) {
        return paperDeliveries.stream()
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY, deliveryWeek))
                .toList();
    }

    public List<PaperDelivery> mapItemForEvaluateDriverCapacityStep(List<PaperDelivery> paperDeliveries, LocalDate deliveryWeek) {
        return paperDeliveries.stream()
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, deliveryWeek))
                .toList();
    }

    public List<PaperDelivery> mapItemForEvaluatePrintCapacityStep(List<PaperDelivery> paperDeliveries, LocalDate deliveryWeek) {
        return paperDeliveries.stream()
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_PRINT_CAPACITY, deliveryWeek))
                .toList();
    }

    public List<PaperDelivery> mapItemForEvaluateSenderLimitOnNextWeek(List<PaperDelivery> paperDeliveries, LocalDate deliveryWeek) {
        return paperDeliveries.stream()
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_SENDER_LIMIT, deliveryWeek.plusWeeks(1)))
                .toList();
    }

    /**
     * This method filters the deliveries based on the remaining capacity and prepares them for the next step.
     * It also handles the case where there are more deliveries than the remaining capacity, moving excess deliveries to the next week.
     *
     * @param deliveries       List of PaperDelivery items to filter
     * @param capCapacities    Tuple containing total capacity and used capacity
     * @param deliveriesToSend List to add filtered deliveries that will be sent
     * @param toNextWeek       List to add deliveries that will be moved to next week
     * @return The number of deliveries that were filtered and prepared
     */
    public Integer filterOnResidualDriverCapacity(List<PaperDelivery> deliveries, Tuple2<Integer, Integer> capCapacities, List<PaperDelivery> deliveriesToSend, List<PaperDelivery> toNextWeek, LocalDate deliveryWeek) {
        int remainingCapacity = Math.max(capCapacities.getT1() - capCapacities.getT2(), 0);
        List<PaperDelivery> filteredList = deliveries.stream().limit(remainingCapacity).toList();

        if (!filteredList.isEmpty()) deliveriesToSend.addAll(mapItemForEvaluatePrintCapacityStep(filteredList, deliveryWeek));
        if (filteredList.size() < deliveries.size()) toNextWeek.addAll(deliveries.subList(filteredList.size(), deliveries.size()));

        return filteredList.size();
    }

    public List<PaperDelivery> assignUnifiedDeliveryDriverAndEnrichWithDriverAndPriority(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses, Map<String, List<PaperDelivery>> groupedByCapProductType, String tenderId, Map<Integer, List<String>> priorityMap) {
        Map<String, String> driverMap = groupByGeoKeyAndProduct(paperChannelDeliveryDriverResponses);
        return groupedByCapProductType.entrySet().stream()
                .flatMap(entry -> Optional.ofNullable(driverMap.get(entry.getKey()))
                        .stream()
                        .flatMap(driver -> enrichWithPriorityAndUnifiedDeliveryDriver(entry.getValue(), driver, tenderId, priorityMap).stream()))
                .toList();
    }

    /**
     * Evaluates the sender limit for each product type and PaId.
     * Based on the sender's limit, it splits the deliveries into two groups:
     * those to be sent to the driver capacity evaluation step,
     * and those to be sent to the residual capacity evaluation step.
     * @param senderLimitMap Map containing the sender limits for each product type and Pa - key in the format "PaId~ProductType~Province"
     * @param deliveriesGroupedByProductTypePaId Map containing the deliveries grouped by product type and Pa
     * @param senderLimitJobPaperDeliveries Object containing the lists to which the deliveries will be
     */
    public void evaluateSenderLimitAndFilterDeliveries(Map<String, Tuple2<Integer, Integer>> senderLimitMap, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        deliveriesGroupedByProductTypePaId.forEach((key, deliveries) -> {
            int limit = Optional.ofNullable(senderLimitMap.get(key))
                    .map(senderLimits -> senderLimits.getT1() - senderLimits.getT2())
                    .orElse(0);

            int actualLimit = Math.min(limit, deliveries.size());
            senderLimitJobPaperDeliveries.getSendToDriverCapacityStep().addAll(new ArrayList<>(deliveries.subList(0, actualLimit)));
            senderLimitJobPaperDeliveries.getSendToResidualCapacityStep().addAll(new ArrayList<>(deliveries.subList(actualLimit, deliveries.size())));
        });
    }

    public List<PaperDelivery> enrichWithPriorityAndUnifiedDeliveryDriver(List<PaperDelivery> deliveries, String unifiedDeliveryDriver, String tenderId, Map<Integer, List<String>> priorityMap) {
        deliveries.forEach(paperDelivery -> {
            Integer priority = findPriorityOnMap(priorityMap, paperDelivery);
            paperDelivery.setUnifiedDeliveryDriver(unifiedDeliveryDriver);
            paperDelivery.setTenderId(tenderId);
            paperDelivery.setPriority(priority);
        });
        return deliveries;
    }

    private static Integer findPriorityOnMap(Map<Integer, List<String>> priorityMap, PaperDelivery paperDelivery) {
        String key = "PRODUCT_" + paperDelivery.getProductType() + ".ATTEMPT_" + paperDelivery.getAttempt();
        return priorityMap.entrySet().stream()
                .filter(entry -> entry.getValue().contains(key))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(3);
    }

    /**
     *
     * @param items spedizioni in input
     * @param senderLimitJobPaperDeliveries contiene nel campo sendToDriverCapacityStep le spedizioni che sono RS e secondo tentativi
     * @return le spedizioni che non sono nè RS nè secondi tentativi
     * <p>
     * partitioned.get(true) -> spedizioni che sono RS o secondi tentativi
     * partitioned.get(false) -> spedizioni che non sono nè RS nè secondi tentativi
     */
    public List<PaperDelivery> excludeRsAndSecondAttempt(List<PaperDelivery> items, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        Predicate<PaperDelivery> shouldExclude = paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1;
        Map<Boolean, List<PaperDelivery>> partitioned = items.stream().collect(Collectors.partitioningBy(shouldExclude));
        senderLimitJobPaperDeliveries.setSendToDriverCapacityStep(partitioned.get(true));
        return partitioned.get(false);
    }

}

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
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

    public Map<String, List<PaperDelivery>> groupByPaIdProductTypeProvince(List<PaperDelivery> paperDeliveries, String province) {
        return paperDeliveries.stream()
                .collect(Collectors.groupingBy(paperDelivery -> paperDelivery.getSenderPaId() + "~" + paperDelivery.getProductType() + "~" + province,
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
                .collect(Collectors.toMap(item -> item.getGeoKey() + "~" + item.getProduct(), PaperChannelDeliveryDriverResponse::getUnifiedDeliveryDriver));
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

    public List<PaperDelivery> assignUnifiedDeliveryDriverAndBuildNewStepEntities(List<PaperChannelDeliveryDriverResponse> paperChannelDeliveryDriverResponses, Map<String, List<PaperDelivery>> groupedByCapProductType, String tenderId, Map<Integer, List<String>> priorityMap) {
        Map<String, String> driverMap = groupByGeoKeyAndProduct(paperChannelDeliveryDriverResponses);
        return groupedByCapProductType.entrySet().stream()
                .flatMap(entry -> Optional.ofNullable(driverMap.get(entry.getKey()))
                        .stream()
                        .flatMap(driver -> enrichWithPriorityAndUnifiedDeliveryDriver(entry.getValue(), driver, tenderId, priorityMap).stream()))
                .toList();
    }

    public SenderLimitJobPaperDeliveries evaluateSenderLimitAndFilter(Map<String, Integer> senderLimitMap, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        deliveriesGroupedByProductTypePaId.forEach((key, deliveries) -> {
            List<PaperDelivery> filteredList = Optional.ofNullable(senderLimitMap.get(key)).map(limit -> deliveries.stream().limit(limit).toList()).orElse(deliveries);
            senderLimitJobPaperDeliveries.getSendToNextStep().addAll(filteredList);
            Optional.of(deliveries.size() - filteredList.size()).filter(remaining -> remaining > 0).ifPresent(remaining -> senderLimitJobPaperDeliveries.getSendToResidualStep().addAll(deliveries.subList(filteredList.size(), deliveries.size())));
        });
        return senderLimitJobPaperDeliveries;
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

    public List<PaperDelivery> excludeRsAndSecondAttempt(List<PaperDelivery> items, SenderLimitJobPaperDeliveries senderLimitJobPaperDeliveries) {
        List<PaperDelivery> sendToResidualStep = new ArrayList<>();
        List<PaperDelivery> sendToNextStep = new ArrayList<>(items.stream().filter(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1).toList());
        items.removeIf(paperDelivery -> paperDelivery.getProductType().equalsIgnoreCase("RS") || paperDelivery.getAttempt() == 1);
        senderLimitJobPaperDeliveries.setSendToResidualStep(sendToResidualStep);
        senderLimitJobPaperDeliveries.setSendToNextStep(sendToNextStep);
        return senderLimitJobPaperDeliveries.getSendToNextStep();
    }

}

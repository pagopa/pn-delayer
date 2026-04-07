package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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
    private final PrintCapacityUtils printCapacityUtils;

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

    public Map<String, String> groupByGeoKeyAndProduct(List<PaperChannelDeliveryDriver> paperChannelDeliveryDriver) {
        return paperChannelDeliveryDriver.stream()
                .collect(Collectors.toMap(item -> item.getGeoKey() + "~" + item.getProduct(), PaperChannelDeliveryDriver::getUnifiedDeliveryDriver, (x, y) -> x));
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
        List<PaperDelivery> filteredList = new ArrayList<>();
        if(remainingCapacity > 0) {
            filteredList.addAll(deliveries.stream().limit(remainingCapacity).toList());
        }

        if (!filteredList.isEmpty()) deliveriesToSend.addAll(mapItemForEvaluatePrintCapacityStep(filteredList, deliveryWeek));
        if (filteredList.size() < deliveries.size()) toNextWeek.addAll(deliveries.subList(filteredList.size(), deliveries.size()));

        return filteredList.size();
    }


    /**
     * Evaluates the sender limit for each product type and PaId.
     * Based on the sender's limit, it splits the deliveries into two groups:
     * those to be sent to the driver capacity evaluation step,
     * and those to be sent to the residual capacity evaluation step.
     * @param senderLimitMap Map containing the sender limits for each product type and Pa - key in the format "PaId~ProductType~Province"
     * @param deliveriesGroupedByProductTypePaId Map containing the deliveries grouped by product type and Pa
     * @param senderLimitJobProcessObjects Object containing the lists to which the deliveries will be
     */
    public void evaluateSenderLimitAndFilterDeliveries(Map<String, Tuple2<Integer, Integer>> senderLimitMap, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
        List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
        deliveriesGroupedByProductTypePaId.forEach((key, deliveries) -> {
            int limit = Optional.ofNullable(senderLimitMap.get(key))
                    .map(senderLimits -> senderLimits.getT1() - senderLimits.getT2())
                    .orElse(0);

            int actualLimit = Math.min(limit, deliveries.size());

            List<PaperDelivery> driverStep = (actualLimit == 0) ? List.of() : new ArrayList<>(deliveries.subList(0, actualLimit));
            List<PaperDelivery> residualStep = (actualLimit >= deliveries.size()) ? List.of() : new ArrayList<>(deliveries.subList(actualLimit, deliveries.size()));

            if (!driverStep.isEmpty()) {
                senderLimitMap.put(key, Tuples.of(
                        Optional.ofNullable(senderLimitMap.get(key)).map(Tuple2::getT1).orElse(0),
                        Optional.ofNullable(senderLimitMap.get(key)).map(Tuple2::getT2).orElse(0) + driverStep.size()));
            }

            sendToDriverCapacityStep.addAll(driverStep);
            sendToResidualCapacityStep.addAll(residualStep);
        });

        senderLimitJobProcessObjects.getSendToResidualCapacityStep().addAll(sendToResidualCapacityStep);
        senderLimitJobProcessObjects.getSendToDriverCapacityStep().addAll(sendToDriverCapacityStep);
    }

    /**
     * Filtra e smista le spedizioni in base al prodotto, tentativo e tipo di comunicazione(LEGAL / INFORMAL).
     * Il metodo esegue separa le spedizioni in INFORMAL e LEGAL.<
     * Tra le LEGAL Le RS o i secondi tentativi vengono inviati allo step EVALUATE_DRIVER_CAPACITY.
     * Le restanti vengono restituite come risultato del metodo
     * Tutte le spedizioni INFORMAL vengono inviate allo step EVALUATE_RESIDUAL_CAPACITY.
     *
     * @param items lista delle spedizioni in input
     * @param senderLimitJobProcessObjects oggetto popolato con le spedizioni suddivise per step di processamento
     * @return lista delle spedizioni non-INFORMAL che non sono né RS né di secondo tentativo
     */
    public List<PaperDelivery> excludeRsAndSecondAttempt(List<PaperDelivery> items, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        var partitionedByCommType = items.stream().collect(Collectors.partitioningBy(this::isInformal));
        var informalItems = partitionedByCommType.get(true);
        var legalItems = partitionedByCommType.get(false);
        var byRsOrSecondAttempt = legalItems.stream().collect(Collectors.partitioningBy(this::isRsOrSecondAttempt));
        senderLimitJobProcessObjects.setSendToDriverCapacityStep(byRsOrSecondAttempt.get(true));
        senderLimitJobProcessObjects.setSendToResidualCapacityStep(
                new ArrayList<>(informalItems)
        );

        return new ArrayList<>(byRsOrSecondAttempt.get(false));
    }

    private boolean isInformal(PaperDelivery paperDelivery) {
        return CommunicationType.INFORMAL.name().equalsIgnoreCase(paperDelivery.getCommunicationType());
    }

    private boolean isRsOrSecondAttempt(PaperDelivery paperDelivery) {
        return ProductType.RS.getValue().equalsIgnoreCase(paperDelivery.getProductType()) || paperDelivery.getAttempt() == 1;
    }

    public Integer retrieveActualPrintCapacity(LocalDate deliveryWeek) {
        return printCapacityUtils.getActualPrintCapacity(deliveryWeek);

    }
}

package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDelivery;
import it.pagopa.pn.delayer.model.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import org.springframework.util.StringUtils;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.*;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class PnDelayerUtils {

    private final PnDelayerConfigs pnDelayerConfig;
    private final PrintCapacityUtils printCapacityUtils;

    /**
     * This method calculates the start day of the delivery week based on the execution batch start date.
     */
    public LocalDate calculateDeliveryWeek(Instant date) {
        LocalDate localDate = date.atZone(ZoneOffset.UTC).toLocalDate();
        return localDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(pnDelayerConfig.getDeliveryDateDayOfWeek())));
    }

    public Map<String, List<PaperDelivery>> groupByCap(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream().collect(Collectors.groupingBy(PaperDelivery::getCap, Collectors.toList()));
    }

    public Map<String, String> groupByGeoKeyAndProduct(List<PaperChannelDeliveryDriver> paperChannelDeliveryDriver) {
        return paperChannelDeliveryDriver.stream()
                .collect(Collectors.toMap(item -> item.getGeoKey() + "~" + item.getProduct(), PaperChannelDeliveryDriver::getUnifiedDeliveryDriver, (x, _) -> x));
    }

    public Map<String, List<PaperDelivery>> groupByCapAndProductType(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream().collect(Collectors.groupingBy(paperDelivery -> paperDelivery.getCap() + "~" + paperDelivery.getProductType()));
    }

    /**
     * Smista e raggruppa le spedizioni in un unico passaggio sulla lista di input, popolando
     * {@link SenderLimitJobProcessObjects} con le liste destinate agli step successivi.
     * <p>
     * Per ogni {@link PaperDelivery} viene applicata, nell'ordine, la prima regola che corrisponde:
     * <ol>
     *     <li>se {@code senderPaId} è assente o vuoto, la spedizione viene aggiunta a
     *     {@code sendToResidualCapacityStep} (verrà processata nello step EVALUATE_RESIDUAL_CAPACITY
     *     perché non è possibile valutare un limite mittente senza paId);</li>
     *     <li>se la comunicazione è INFORMAL, la spedizione viene aggiunta a
     *     {@code sendToResidualCapacityStep} (le INFORMAL bypassano la valutazione del sender limit);</li>
     *     <li>se il prodotto è RS oppure si tratta di un secondo tentativo ({@code attempt == 1}),
     *     la spedizione viene aggiunta a {@code sendToDriverCapacityStep} (va direttamente allo step
     *     EVALUATE_DRIVER_CAPACITY, senza consumare il sender limit);</li>
     *     <li>negli altri casi la spedizione viene inserita nella mappa di output, raggruppata per
     *     chiave {@code senderPaId~productType~province}, mantenendo l'ordine di input all'interno
     *     di ogni gruppo. Questa mappa è l'input della valutazione del sender limit.</li>
     * </ol>
     * Le liste {@code sendToResidualCapacityStep} e {@code sendToDriverCapacityStep} vengono
     * sostituite (non accodate) sull'oggetto di processo: la chiamata successiva a
     * {@link #evaluateSenderLimitAndFilterDeliveries(Map, Map, SenderLimitJobProcessObjects, LocalDate)}
     * provvederà ad aggiungere ulteriori spedizioni a queste stesse liste.
     * <p>
     *
     * @param paperDeliveries lista delle spedizioni arricchite con driver e priorità da smistare
     * @param senderLimitJobProcessObjects oggetto di processo in cui scrivere le liste destinate
     *                                     agli step EVALUATE_RESIDUAL_CAPACITY e
     *                                     EVALUATE_DRIVER_CAPACITY
     * @return mappa delle spedizioni candidate alla valutazione del sender limit, raggruppate
     *         per chiave {@code senderPaId~productType~province}
     */
    public Map<String, List<PaperDelivery>> classifyAndGroupForSenderLimit(List<PaperDelivery> paperDeliveries, SenderLimitJobProcessObjects senderLimitJobProcessObjects) {
        List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
        List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
        Map<String, List<PaperDelivery>> groupedForSenderLimit = new HashMap<>();

        for (PaperDelivery paperDelivery : paperDeliveries) {
            if (!StringUtils.hasText(paperDelivery.getSenderPaId())) {
                sendToResidualCapacityStep.add(paperDelivery);
            } else if (isInformal(paperDelivery)) {
                sendToResidualCapacityStep.add(paperDelivery);
            } else if (needToSkipSenderLimit(paperDelivery)) {
                sendToDriverCapacityStep.add(paperDelivery);
            } else {
                String key = paperDelivery.getSenderPaId() + "~" + paperDelivery.getProductType() + "~" + paperDelivery.getProvince();
                groupedForSenderLimit.computeIfAbsent(key, _ -> new ArrayList<>()).add(paperDelivery);
            }
        }

        senderLimitJobProcessObjects.setSendToResidualCapacityStep(sendToResidualCapacityStep);
        senderLimitJobProcessObjects.setSendToDriverCapacityStep(sendToDriverCapacityStep);

        return groupedForSenderLimit;
    }

    private boolean needToSkipSenderLimit(PaperDelivery paperDelivery) {
        return pnDelayerConfig.isEnablePriorityResidualFlow() ? paperDelivery.isSkipSenderLimit() : paperDelivery.isLegacySkipSenderLimit();
    }

    public Map<String, Long> groupingForExclude(List<PaperDelivery> paperDeliveries) {
        return paperDeliveries.stream()
                .filter(paperDelivery -> paperDelivery.isSkipSenderLimit() && !CommunicationType.INFORMAL.name().equals(paperDelivery.getCommunicationType()))
                .collect(Collectors.groupingBy(paperDelivery -> String.join("~", paperDelivery.getProvince(), paperDelivery.getProductType()), Collectors.counting()));
    }

    public List<PaperDelivery> mapItemForResidualCapacityStep(SenderLimitJobProcessObjects senderLimitJobProcessObjects, LocalDate deliveryWeek) {
        return senderLimitJobProcessObjects.getSendToResidualCapacityStep().stream()
                .map(paperDelivery -> evaluateAndSetSkipSenderLimit(paperDelivery, senderLimitJobProcessObjects, deliveryWeek, true))
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_RESIDUAL_CAPACITY, deliveryWeek))
                .toList();
    }


    public List<PaperDelivery> mapItemForEvaluateDriverCapacityStep(SenderLimitJobProcessObjects senderLimitJobProcessObjects, LocalDate deliveryWeek) {
        return senderLimitJobProcessObjects.getSendToDriverCapacityStep().stream()
                .map(paperDelivery -> evaluateAndSetSkipSenderLimit(paperDelivery, senderLimitJobProcessObjects, deliveryWeek, false))
                .map(paperDelivery -> new PaperDelivery(paperDelivery, WorkflowStepEnum.EVALUATE_DRIVER_CAPACITY, deliveryWeek))
                .toList();
    }

    private PaperDelivery evaluateAndSetSkipSenderLimit(PaperDelivery paperDelivery, SenderLimitJobProcessObjects processObjects, LocalDate deliveryWeek, boolean isResidual) {
        if (paperDelivery.isSkipSenderLimit()) {
            return paperDelivery;
        }
        if (!StringUtils.hasText(paperDelivery.getSenderPaId())) {
            paperDelivery.setSkipSenderLimit(false);
            return paperDelivery;
        }
        if (isInformal(paperDelivery)) {
            paperDelivery.setSkipSenderLimit(false);
            return paperDelivery;
        }
        if (paperDelivery.isDelayed() && paperDelivery.getPreviousStep() == null) {
            applyWeeklySenderLimit(paperDelivery, processObjects, calculateDeliveryWeek(Instant.parse(paperDelivery.getNotificationSentAt())).toString(), isResidual);
            return paperDelivery;
        }
        applyWeeklySenderLimit(paperDelivery, processObjects, deliveryWeek.minusWeeks(1).toString(), isResidual);
        return paperDelivery;
    }

    private void applyWeeklySenderLimit(PaperDelivery paperDelivery, SenderLimitJobProcessObjects processObjects, String shipmentDate, boolean isResidual) {
        String key = String.join("~", shipmentDate, paperDelivery.getSenderPaId(), paperDelivery.getProductType(), paperDelivery.getProvince());
        Map<String, SenderLimitData> senderLimitMap = processObjects.getSenderLimitMap();
        SenderLimitData limitData = senderLimitMap.get(key);
        if (Objects.nonNull(limitData) && limitData.weeklyEstimate() > 0) {
            int availableLimit = limitData.weeklyEstimate() - limitData.totalUsedLimit();
            boolean hasAvailableLimit = availableLimit > 0;
            paperDelivery.setSkipSenderLimit(hasAvailableLimit);
            if (hasAvailableLimit && isResidual) {
                senderLimitMap.put(key, limitData.incrementUsedLimit(1));
            }
        }
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
    public void evaluateSenderLimitAndFilterDeliveries(Map<String, SenderLimitData> senderLimitMap, Map<String, List<PaperDelivery>> deliveriesGroupedByProductTypePaId, SenderLimitJobProcessObjects senderLimitJobProcessObjects, LocalDate deliveryWeek) {
        String shipmentDate = deliveryWeek.minusWeeks(1).toString();
        List<PaperDelivery> sendToDriverCapacityStep = new ArrayList<>();
        List<PaperDelivery> sendToResidualCapacityStep = new ArrayList<>();
        deliveriesGroupedByProductTypePaId.forEach((key, deliveries) -> {
            SenderLimitData senderLimitData = senderLimitMap.get(String.join("~", shipmentDate, key));
            int limit = Optional.ofNullable(senderLimitData)
                    .map(SenderLimitData::availableLimit)
                    .orElse(0);

            int actualLimit = Math.min(limit, deliveries.size());

            List<PaperDelivery> driverStep = (actualLimit == 0) ? List.of() : new ArrayList<>(deliveries.subList(0, actualLimit));
            List<PaperDelivery> residualStep = (actualLimit >= deliveries.size()) ? List.of() : new ArrayList<>(deliveries.subList(actualLimit, deliveries.size()));

            if (!driverStep.isEmpty()) {
                senderLimitMap.computeIfPresent(String.join("~", shipmentDate, key), (ignoredKey, currentSenderLimit) ->
                        currentSenderLimit.incrementUsedLimit(driverStep.size()));
            }

            sendToDriverCapacityStep.addAll(driverStep);
            sendToResidualCapacityStep.addAll(residualStep);
        });

        senderLimitJobProcessObjects.getSendToResidualCapacityStep().addAll(sendToResidualCapacityStep);
        senderLimitJobProcessObjects.getSendToDriverCapacityStep().addAll(sendToDriverCapacityStep);
    }


    private boolean isInformal(PaperDelivery paperDelivery) {
        return CommunicationType.INFORMAL.name().equalsIgnoreCase(paperDelivery.getCommunicationType());
    }

    public Integer retrieveActualPrintCapacity(LocalDate deliveryWeek) {
        return printCapacityUtils.getActualPrintCapacity(deliveryWeek);

    }
}

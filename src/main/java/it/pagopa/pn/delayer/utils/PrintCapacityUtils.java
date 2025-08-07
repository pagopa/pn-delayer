package it.pagopa.pn.delayer.utils;

import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.exception.PnPrintCapacityNotFound;
import it.pagopa.pn.delayer.model.PrintCapacity;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Component
@Slf4j
public class PrintCapacityUtils {

    private static final String SEPARATOR = ";";
    private static final int START_DATE_INDEX = 0;

    private final List<PrintCapacity> printCapacities;


    public PrintCapacityUtils(PnDelayerConfigs config) {
        printCapacities = buildPrintCapacityItemFromStringList(config.getPrintCapacity());
    }

    private List<PrintCapacity> buildPrintCapacityItemFromStringList(List<String> printCapacities) {
        if(CollectionUtils.isEmpty(printCapacities)){
            return Collections.emptyList();
        }
        return printCapacities.stream()
                .map(this::toPrintCapacity)
                .sorted(Comparator.comparing(PrintCapacity::startConfigurationTime).reversed())
                .toList();

    }

    private PrintCapacity toPrintCapacity(String printCapacity) {
        String[] printCapacitySplit = printCapacity.split(SEPARATOR);
        LocalDate startDate = LocalDate.parse(printCapacitySplit[START_DATE_INDEX]);
        return new PrintCapacity(startDate, Integer.parseInt(printCapacitySplit[1]));
    }

    public Integer getActualPrintCapacity(LocalDate deliveryWeek) {
        return printCapacities.stream()
                .filter(printCapacity -> deliveryWeek.isAfter(printCapacity.startConfigurationTime()))
                .findFirst()
                .map(PrintCapacity::printCapacity)
                .orElseThrow(() -> new PnPrintCapacityNotFound(deliveryWeek));
    }


    @PostConstruct
    public void validate() {
        log.debug("Validating chargeCalculationModes...");
        getActualPrintCapacity(LocalDate.now());
        log.debug("Validated chargeCalculationModes");
    }
}

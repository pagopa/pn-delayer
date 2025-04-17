package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.pn.delayer.config.PnDelayerConfigs;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryReadyToSend;
import it.pagopa.pn.delayer.model.ImplementationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static it.pagopa.pn.delayer.config.PnDelayerConfigs.IMPLEMENTATION_TYPE_PROPERTY_NAME;

@Component
@Slf4j
@ConditionalOnProperty(name = IMPLEMENTATION_TYPE_PROPERTY_NAME, havingValue = ImplementationType.INMEMORY)
public class PaperDeliveryHighPriorityInMemoryDbImpl implements PaperDeliveryHighPriorityDAO {

    private final PnDelayerConfigs pnDelayerConfigs;
    private final PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSendDAO;

    private final ConcurrentHashMap<String, List<PaperDeliveryHighPriority>> data = new ConcurrentHashMap<>();

    public PaperDeliveryHighPriorityInMemoryDbImpl(PnDelayerConfigs pnDelayerConfigs,
                                                   PaperDeliveryReadyToSendInMemoryDbImpl paperDeliveryReadyToSendDAO,
                                                   ObjectMapper objectMapper) throws IOException {
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryHighPriority.json");
        List<PaperDeliveryHighPriority> highPriorityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {});
        data.putAll(highPriorityList.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getUnifiedDeliveryDriverGeoKey,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(PaperDeliveryHighPriority::getCreatedAt));
                                    return list;
                                }
                        )
                )));
        log.info("loaded {} PaperDeliveryHighPriority", highPriorityList.size());
        this.pnDelayerConfigs = pnDelayerConfigs;
        this.paperDeliveryReadyToSendDAO = paperDeliveryReadyToSendDAO;
    }

    public Mono<Void> delete(String pk, List<PaperDeliveryHighPriority> toRemove) {
        data.compute(pk, (k, existingList) -> {
            if (CollectionUtils.isEmpty(existingList)) {
                log.info("No PaperDeliveryHighPriority found for pk: {}. Nothing to delete.", pk);
                return Collections.emptyList();
            } else {
                log.info("Deleting {} PaperDeliveryHighPriority from pk: {}", toRemove.size(), pk);
                List<PaperDeliveryHighPriority> updatedList = new ArrayList<>(existingList);
                updatedList.removeAll(toRemove);
                return updatedList;
            }
        });
        return Mono.empty();
    }

    public List<PaperDeliveryHighPriority> get(String pk) {
        return Optional.ofNullable(data.get(pk))
                .orElse(Collections.emptyList());
    }

    @Override
    public Mono<Page<PaperDeliveryHighPriority>> getPaperDeliveryHighPriority(String unifiedDeliveryDriver, String geoKey, Map<String, AttributeValue> lastEvaluatedKey) {
        List<PaperDeliveryHighPriority> highPriorities = data.get(PaperDeliveryHighPriority.buildKey(unifiedDeliveryDriver, geoKey));
        if(CollectionUtils.isEmpty(highPriorities)) {
            log.info("No PaperDeliveryHighPriority found for unifiedDeliveryDriver: {}, geoKey: {}", unifiedDeliveryDriver, geoKey);
            return Mono.just(Page.create(Collections.emptyList()));
        }
        log.info("Found {} PaperDeliveryHighPriority for unifiedDeliveryDriver: {}, geoKey: {}", highPriorities.size(), unifiedDeliveryDriver, geoKey);
        return Mono.just(Page.create(highPriorities
                .stream().limit(pnDelayerConfigs.getHighPriorityQueryLimit()).toList()));
    }

    @Override
    public Mono<Void> executeTransaction(List<PaperDeliveryHighPriority> paperDeliveryHighPriority, List<PaperDeliveryReadyToSend> paperDeliveryReadyToSend) {
        return paperDeliveryReadyToSendDAO.insert(paperDeliveryReadyToSend)
                .doOnNext(savedItems -> log.info("Inserted {} PaperDeliveryReadyToSend", savedItems))
                .flatMap(savedItems -> delete(paperDeliveryHighPriority.get(0).getUnifiedDeliveryDriverGeoKey(), paperDeliveryHighPriority))
                .doOnNext(deletedItems -> log.info("Deleted {} PaperDeliveryHighPriority", paperDeliveryHighPriority.size()));
    }
}

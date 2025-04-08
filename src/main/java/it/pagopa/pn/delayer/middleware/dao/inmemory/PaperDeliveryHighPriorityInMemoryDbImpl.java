package it.pagopa.pn.delayer.middleware.dao.inmemory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryHighPriorityDAO;
import it.pagopa.pn.delayer.middleware.dao.PaperDeliveryReadyToSendDAO;
import it.pagopa.pn.delayer.middleware.dao.dynamo.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.model.PaperDeliveryTransactionRequest;
import it.pagopa.pn.delayer.utils.PaperDeliveryUtils;
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

@Component
public class PaperDeliveryHighPriorityInMemoryDbImpl implements PaperDeliveryHighPriorityDAO {

    private final PaperDeliveryUtils paperDeliveryUtils;
    private final PaperDeliveryReadyToSendDAO paperDeliveryReadyToSendDAO;

    private final ConcurrentHashMap<String, List<PaperDeliveryHighPriority>> data = new ConcurrentHashMap<>();

    public PaperDeliveryHighPriorityInMemoryDbImpl(PaperDeliveryUtils paperDeliveryUtils, PaperDeliveryReadyToSendDAO paperDeliveryReadyToSendDAO) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        ClassPathResource classPathResource = new ClassPathResource("json/PaperDeliveryHighPriority.json");
        List<PaperDeliveryHighPriority> highPriorityList = objectMapper.readValue(classPathResource.getFile(), new TypeReference<>() {});
        data.putAll(highPriorityList.stream()
                .collect(Collectors.groupingBy(
                        PaperDeliveryHighPriority::getPk,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> {
                                    list.sort(Comparator.comparing(PaperDeliveryHighPriority::getCreatedAt));
                                    return list;
                                }
                        )
                )));
        this.paperDeliveryUtils = paperDeliveryUtils;
        this.paperDeliveryReadyToSendDAO = paperDeliveryReadyToSendDAO;
    }

    @Override
    public Mono<Integer> delete(String pk, List<PaperDeliveryHighPriority> toRemove) {
        data.compute(pk, (k, existingList) -> {
            if (CollectionUtils.isEmpty(existingList)) {
                return Collections.emptyList();
            } else {
                List<PaperDeliveryHighPriority> updatedList = new ArrayList<>(existingList);
                updatedList.removeAll(toRemove);
                return updatedList;
            }
        });
        return Mono.just(toRemove.size());
    }

    @Override
    public List<PaperDeliveryHighPriority> get(String pk) {
        return Optional.ofNullable(data.get(pk))
                .orElse(Collections.emptyList());
    }

    @Override
    public Mono<Page<PaperDeliveryHighPriority>> getChunck(String pk, int limit, Map<String, AttributeValue> lastEvaluatedKey) {
        List<PaperDeliveryHighPriority> highPriorities = data.get(pk);
        if(CollectionUtils.isEmpty(highPriorities)) {
            return Mono.just(Page.create(Collections.emptyList()));
        }
        return Mono.just(Page.create(highPriorities
                .stream().limit(limit).toList()));
    }

    @Override
    public Mono<Integer> executeTransaction(PaperDeliveryTransactionRequest paperDeliveryTransactionRequest) {
        return paperDeliveryReadyToSendDAO.insert(paperDeliveryTransactionRequest.getPaperDeliveryReadyToSendList())
                .flatMap(savedItems -> delete(paperDeliveryTransactionRequest.getPaperDeliveryHighPriorityList().get(0).getPk(),
                        paperDeliveryTransactionRequest.getPaperDeliveryHighPriorityList()));
    }

    public List<PaperDeliveryHighPriority> getAll() {
        return data.values().stream().flatMap(Collection::stream).toList();
    }
}

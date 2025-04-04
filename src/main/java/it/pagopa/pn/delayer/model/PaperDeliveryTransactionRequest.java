package it.pagopa.pn.delayer.model;

import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryHighPriority;
import it.pagopa.pn.delayer.middleware.dao.entity.PaperDeliveryReadyToSend;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class PaperDeliveryTransactionRequest {
    List<PaperDeliveryHighPriority> paperDeliveryHighPriorityList = new ArrayList<>();
    List<PaperDeliveryReadyToSend> paperDeliveryReadyToSendList = new ArrayList<>();
}

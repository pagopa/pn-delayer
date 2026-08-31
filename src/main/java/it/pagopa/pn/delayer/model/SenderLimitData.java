package it.pagopa.pn.delayer.model;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Rappresenta le informazioni relative al modulo commessa e al limite garantito
 * al mittente per una specifica settimana di riferimento.
 *
 * @param weeklyEstimate stima settimanale derivante dal modulo commessa del mittente
 * @param calculatedLimit limite garantito calcolato per il mittente in funzione della capacità di recapito disponibile
 *                        nella provincia per la settimana di riferimento
 * @param incrementUsedLimit quota del sender limit utilizzata dal mittente durante l'esecuzione corrente
 *                           dell'algoritmo e non ancora persistita
 * @param date settimana di riferimento del sender limit
 */
public record SenderLimitData(Integer weeklyEstimate, Integer calculatedLimit, Integer incrementUsedLimit, LocalDate date) {

    public SenderLimitData {
        weeklyEstimate = Objects.requireNonNullElse(weeklyEstimate, 0);
        calculatedLimit = Objects.requireNonNullElse(calculatedLimit, 0);
        incrementUsedLimit = Objects.requireNonNullElse(incrementUsedLimit, 0);
    }

    /**
     * Crea un'istanza iniziale del sender limit per il flusso standard di elaborazione.
     * <p>
     * Viene utilizzata per le spedizioni elaborate nella settimana di competenza,
     * per le quali non è necessario recuperare una quota di sender limit già
     * utilizzata da precedenti esecuzioni dell'algoritmo. In questo caso la
     * quota accumulata durante l'elaborazione corrente
     * ({@code incrementUsedLimit}) viene inizializzata a zero.
     *
     * @param weeklyEstimate stima settimanale derivante dal modulo commessa del mittente
     * @param calculatedLimit limite garantito calcolato per il mittente per la settimana di riferimento
     * @param date settimana di riferimento
     * @return una nuova istanza inizializzata per il flusso standard
     */
    public static SenderLimitData initial(Integer weeklyEstimate, Integer calculatedLimit, LocalDate date) {
        return new SenderLimitData(weeklyEstimate, calculatedLimit, 0, date);
    }

    /**
     * Calcola la capacità residua del mittente.
     * <p>
     * Il valore restituito è dato dalla differenza tra il limite calcolato
     * e il numero totale di spedizioni utilizzate.
     * <p>
     * Il risultato non può mai essere negativo: nel caso in cui le spedizioni
     * utilizzate eccedano il limite disponibile viene restituito {@code 0}.
     *
     * @return capacità residua disponibile
     */
    public int availableLimit() {
        return Math.max(calculatedLimit - incrementUsedLimit, 0);
    }

    /**
     * Restituisce una nuova istanza con il numero di spedizioni accumulate
     * incrementato della quantità specificata.
     * <p>
     * L'istanza corrente non viene modificata, in quanto il record è immutabile.
     *
     * @param quantity numero di spedizioni da aggiungere
     * @return una nuova istanza con il contatore aggiornato
     * @throws IllegalArgumentException se la quantità è negativa
     */
    public SenderLimitData incrementUsedLimit(int quantity) {
        if (quantity < 0) {
            throw new IllegalArgumentException("The quantity used to increment the limit cannot be negative");
        }

        return new SenderLimitData(
                weeklyEstimate,
                calculatedLimit,
                incrementUsedLimit + quantity,
                date
        );
    }
}
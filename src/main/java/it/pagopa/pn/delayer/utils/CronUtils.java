package it.pagopa.pn.delayer.utils;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class CronUtils {

    public static int countExecutionsInNextScheduledDay(String awsCron) {
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
        Cron cron = parser.parse("0 " + awsCron);
        cron.validate();

        ExecutionTime execTime = ExecutionTime.forCron(cron);

        // 2. partiamo da oggi a mezzanotte nel fuso desiderato
        ZonedDateTime startOfToday = LocalDate.now(ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC);

        // 3. chiediamo la prossima esecuzione (potrebbe essere oggi se oggi è il giorno giusto,
        //    oppure il prossimo lunedì, o il 1° del mese, ecc.)
        Optional<ZonedDateTime> firstOpt = execTime.nextExecution(startOfToday.minusSeconds(1));
        if (firstOpt.isEmpty()) return 0;

        LocalDate targetDay = firstOpt.get().toLocalDate();

        // 4. ora facciamo il conteggio in QUEL giorno
        ZonedDateTime startOfDay = targetDay.atStartOfDay(ZoneOffset.UTC);
        ZonedDateTime endOfDay = targetDay.plusDays(1).atStartOfDay(ZoneOffset.UTC).minusNanos(1);

        return countExecutionsInInterval(execTime, startOfDay, endOfDay);
    }


    private static int countExecutionsInInterval(ExecutionTime execTime,
                                                 ZonedDateTime start,
                                                 ZonedDateTime end) {
        return Stream.iterate(
                        start,
                        Objects::nonNull,
                        cursor -> execTime.nextExecution(cursor)
                                .filter(next -> !next.isAfter(end))
                                .map(next -> next.plusSeconds(1))
                                .orElse(null)
                )
                .limit(20_000)
                .skip(1)
                .takeWhile(Objects::nonNull)
                .mapToInt(x -> 1)
                .sum();
    }
}

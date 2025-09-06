import au.com.xing.util.ReferenceDataLoader;
import au.com.xing.generator.TransactionGenerator;
import au.com.xing.kafka.KafkaProducerService;

import java.io.IOException;
import java.util.List;

public class FinancialTransactionProducer {

    public static void main(String[] args) {
        try {
            List<String> australianBanks = ReferenceDataLoader.loadList("/reference-data/bsb.json");
            List<String> transactionTypes = ReferenceDataLoader.loadList("/reference-data/trans-types.json");
            List<String> firstNames = ReferenceDataLoader.loadList("/reference-data/first-names.json");
            List<String> lastNames = ReferenceDataLoader.loadList("/reference-data/last-names.json");
            TransactionGenerator generator = new TransactionGenerator(
                    australianBanks, transactionTypes, firstNames, lastNames
            );
            KafkaProducerService producerService = new KafkaProducerService(generator);
            producerService.produce();
        } catch (IOException e) {
            System.err.println("Failed to init producer: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); // Exit with error code 1
        }
    }
}
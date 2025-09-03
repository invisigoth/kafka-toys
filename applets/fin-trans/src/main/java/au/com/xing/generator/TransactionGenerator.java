package au.com.xing.generator;

import java.io.IOException;
import java.util.List;
import org.apache.commons.math3.random.MersenneTwister; // prefer MT19937 random
import java.util.concurrent.ThreadLocalRandom;
import au.com.xing.util.ReferenceDataLoader;

public class TransactionGenerator {

    private List<String> australianBanks;
    private List<String> transactionTypes;
    private List<String> firstNames;
    private List<String> lastNames;

    private final MersenneTwister random = new MersenneTwister(System.currentTimeMillis());

    public TransactionGenerator() throws IOException {
        try {
            initReferenceData();
        } catch (IOException e) {
            System.err.println("Failed to init reference data: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void initReferenceData() throws IOException {
        australianBanks = ReferenceDataLoader.loadList("/reference-data/bsb.json");
        transactionTypes = ReferenceDataLoader.loadList("/reference-data/trans-types.json");
        firstNames = ReferenceDataLoader.loadList("/reference-data/first-names.json");
        lastNames = ReferenceDataLoader.loadList("/reference-data/last-names.json");
    }

    public String generateBSB() {
        String bankCode = australianBanks.get(random.nextInt(australianBanks.size()));
        String branchCode = String.format("%03d", random.nextInt(1000));
        return bankCode + branchCode;
    }

    public String generateAccountNumber() {
        return String.format("%08d", random.nextInt(100000000));
    }

    public String generateAccountName() {
        String firstName = firstNames.get(random.nextInt(firstNames.size()));
        String lastName = lastNames.get(random.nextInt(lastNames.size()));
        return firstName + " " + lastName;
    }

    public double generateAmount() {
        if (random.nextDouble() < 0.3) {
            return ThreadLocalRandom.current().nextDouble(100000, 500000);
        } else if (random.nextDouble() < 0.1) {
            return ThreadLocalRandom.current().nextDouble(5000, 25000);
        } else {
            return ThreadLocalRandom.current().nextDouble(10, 2000);
        }
    }

    public String generateTransactionReference() {
        String type = transactionTypes.get(random.nextInt(transactionTypes.size()));
        String reference = String.format("REF-%06d", random.nextInt(1000000));
        return type + " - " + reference;
    }
}
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.SplittableRandom;

public class GenerateData {
    private static final String LETTERS = 
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static void main(String[] args) throws IOException {
        Path outputDirectory = Paths.get(args.length > 0? args[0] : "data");
        int customerCount = args.length > 1 ? Integer.parseInt(args[1]) : 50_000;
        int transactionCount = args.length > 2 ? Integer.parseInt(args[2]) : 5_000_000;

        Files.createDirectories(outputDirectory);

        Path customerFile = outputDirectory.resolve("customers.csv");
        Path transactionFile = outputDirectory.resolve("transactions.csv");

        SplittableRandom random = new SplittableRandom();
        DecimalFormat money = new DecimalFormat(
                "0.00", DecimalFormatSymbols.getInstance(Locale.US));

        try (BufferedWriter customers = Files.newBufferedWriter(
                    customerFile, StandardCharsets.UTF_8);
             BufferedWriter transactions = Files.newBufferedWriter(
                    transactionFile, StandardCharsets.UTF_8)) {

            // Customers: ID, Name, Age, Gender, countryCode, Salary
            for (int id = 1; id <= customerCount; id++) {
                String name = randomText(random, random.nextInt(10, 21));
                int age = random.nextInt(10, 71);
                String gender = random.nextBoolean() ? "male" : "female";
                int countryCode = random.nextInt(1, 11);
                double salary = random.nextDouble(100.0, 10_000.0);

                customers.write(id + "," + name + "," + age + "," + gender + "," + countryCode + "," + money.format(salary));
                customers.newLine();
            }
            System.out.println("Genearted " + customerCount + "customers.");

            // Transactions: TransID, CustID, TransTotal, TransNumItems, TransDesc
            for (int transId = 1; transId <= transactionCount; transId++) {
                int customerId = random.nextInt(1, customerCount + 1);
                double total = random.nextDouble(10.0, 1_000.0);
                int itemCount = random.nextInt(1, 11);
                String description = randomText(random, random.nextInt(20, 51));

                transactions.write(transId + "," + customerId + "," + money.format(total) + "," + itemCount + "," + description);
                transactions.newLine();

                if (transId % 1_000_000 == 0) {
                    System.out.println("Generated " + transId + " transactions. ");
                }
            }
        }
        System.out.println("Done. Files written to " + outputDirectory.toAbsolutePath());
    }    

private static String randomText(SplittableRandom random, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
        sb.append(LETTERS.charAt(random.nextInt(LETTERS.length())));
    }
    return sb.toString();
   }
}
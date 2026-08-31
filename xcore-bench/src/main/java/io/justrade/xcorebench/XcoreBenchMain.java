package io.justrade.xcorebench;

/**
 * Comparative benchmark CLI: justrade vs exchange-core.
 *
 * <pre>{@code
 * ./gradlew :xcore-bench:run --args="--mode=book --commands=100000"
 * ./gradlew :xcore-bench:run --args="--mode=engine --warmup=5000 --ops=20000"
 * ./gradlew :xcore-bench:run --args="--mode=e2e --warmup=5000 --ops=20000"
 * ./gradlew :xcore-bench:run --args="--mode=all"
 * }</pre>
 */
public final class XcoreBenchMain {

    private XcoreBenchMain() {}

    public static void main(final String[] args) throws Exception {
        String mode = "book";
        int commands = 100_000;
        int targetOrders = 1_000;
        int users = 1_000;
        int iterations = 3;
        int seed = 1;
        int warmup = 5_000;
        int ops = 20_000;

        for (final String arg : args) {
            final int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            final String key = arg.substring(0, eq);
            final String value = arg.substring(eq + 1);
            switch (key) {
                case "--mode" -> mode = value;
                case "--commands" -> commands = Integer.parseInt(value);
                case "--target-orders" -> targetOrders = Integer.parseInt(value);
                case "--users" -> users = Integer.parseInt(value);
                case "--iterations" -> iterations = Integer.parseInt(value);
                case "--seed" -> seed = Integer.parseInt(value);
                case "--warmup" -> warmup = Integer.parseInt(value);
                case "--ops" -> ops = Integer.parseInt(value);
                default -> throw new IllegalArgumentException("unknown argument: " + key);
            }
        }

        switch (mode) {
            case "book" -> System.out.println(BookComparison.run(new BookComparison.BookComparisonConfig(
                    commands, targetOrders, users, iterations, seed, false, false)));
            case "engine" -> System.out.println(EngineComparison.run(warmup, ops));
            case "e2e" -> System.out.println(E2eComparison.run(warmup, ops));
            case "all" -> {
                System.out.println(BookComparison.run(new BookComparison.BookComparisonConfig(
                        commands, targetOrders, users, iterations, seed, false, false)));
                System.out.println(EngineComparison.run(warmup, ops));
                System.out.println(E2eComparison.run(warmup, ops));
            }
            default -> throw new IllegalArgumentException("unknown mode: " + mode + " (expected: book|engine|e2e|all)");
        }
    }
}

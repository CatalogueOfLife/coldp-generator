package org.catalogueoflife.data;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * Checklist build command
 */
public class GeneratorCLI {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorCLI.class);

    public static void main(String[] args) {
        GeneratorConfig cfg = new GeneratorConfig();
        try {
            new JCommander(cfg, args);

            LOG.info("Building {} archive", cfg.source);
            Class<? extends AbstractGenerator> abClass = cfg.builderClass();
            Constructor<? extends AbstractGenerator> cons = abClass.getConstructor(GeneratorConfig.class);
            AbstractGenerator builder = cons.newInstance(cfg);
            builder.run();
            LOG.info("{} archive completed", cfg.source);

        } catch (Throwable e) {
            // Catch Throwable, not just Exception: linkage problems such as NoSuchFieldError are Errors
            // and would otherwise escape main uncaught.
            LOG.error("Failed to build {} archive", cfg.source, e);
            System.exit(1);
        }
        // Force JVM termination. A generator can leave non-daemon library threads (http client,
        // parsers, …) running; without an explicit exit those keep the process alive forever, which
        // hangs the sequential cron job on a single failing source. System.exit always terminates.
        System.exit(0);
    }

}

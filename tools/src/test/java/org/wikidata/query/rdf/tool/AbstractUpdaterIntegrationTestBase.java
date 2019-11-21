package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.test.CloseableRule.autoClose;
import static org.wikidata.query.rdf.tool.rdf.RdfRepository.UpdateMode.NON_MERGING;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.test.CloseableRule;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.MetricRegistry;

/**
 * Superclass for tests that need to run a full update.
 */
public class AbstractUpdaterIntegrationTestBase {
    /**
     * Wikibase test against.
     */
    @Rule
    public final CloseableRule<WikibaseRepository> wikibaseRepository = autoClose(new WikibaseRepository("https://www.wikidata.org", new MetricRegistry()));
    /**
     * Munger to test against.
     */
    private final Munger munger = Munger.builder(UrisSchemeFactory.getURISystem())
            .removeSiteLinks()
            .build();

    /**
     * Repository to test with.
     */
    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq", NON_MERGING);


    /**
     * Update all ids from from to to.
     * @throws Exception
     */
    @SuppressWarnings("checkstyle:IllegalCatch")
    public void update(int from, int to) {
        ExecutorService executorService = new ThreadPoolExecutor(0, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        ExecutorService importerExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(5));
        UrisScheme uris = UrisSchemeFactory.forHost("www.wikidata.org");
        try (
            Change.Source<?> source = IdRangeChangeSource.forItems(from, to, 30);
            Updater<?> updater = new Updater<>(
                    source, wikibaseRepository.get(), rdfRepository, munger, executorService, importerExecutor, 0,
                    uris, false, new MetricRegistry())
        ) {
            updater.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update the specified id.
     * @throws Exception
     */
    public void update(int id) {
        update(id, id);
    }
}

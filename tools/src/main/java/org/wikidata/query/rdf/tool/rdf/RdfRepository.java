package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.stream.Collectors.toSet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import com.google.common.collect.ImmutableSetMultimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for communicating with the RDF repository.
 */
public class RdfRepository {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);
    /**
     * A special logger to keep track on which Entity IDs were updated.
     * It should be configured to write data to a separate log file,
     * rotating by either day or amount of data (for example 10-100M)
     * overwriting old files to prevent disk overflow, but space provisioned
     * should provide storage for at least several days for issues analysis.
     */
    private static final Logger UPDATED_ENTITY_IDS_LOG = LoggerFactory.getLogger("UPDATED_ENTITY_IDS");
    /**
     * How many statements we will send to RDF processor at once.
     * We assume typical triple line size is under 200 bytes.
     * Each statement appears twice in the output data. So that's how we derive the statement limit.
     * TODO: maybe we can do fine with just one limit?
     */
    private final long maxStatementsPerBatch;

    /**
     * Max statement data size.
     * Entity data repeats twice plus we're taking 1M safety buffer for other data.
     */
    private final long maxPostDataSize;

    /**
     * Uris for wikibase.
     */
    private final UrisScheme uris;

    /**
     * SPARQL for a portion of the update, batched sync.
     */
    private final String msyncBody;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getValues;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getRefs;
    /**
     * SPARQL for a portion of the update.
     */
    private final String cleanUnused;
    /**
     * SPARQL to sync the left off time.
     */
    private final String updateLeftOffTimeBody;
    /**
     * SPARQL to filter entities for newer revisions.
     */
    private final String getRevisions;
    /**
     * SPARQL to verify update worked.
     */
    private final String verifyExtra;
    /**
     * SPARQL to verify update worked.
     */
    private final String verifyMissing;
    /**
     * SPARQL to get lexeme sub-ids.
     */
    private final String getLexemes;

    protected final RdfClient rdfClient;
    /**
     * Should we use merging update endpoint?
     */
    private final UpdateMode updateMode;

    /**
     * @param maxPostSize Max POST form content size.
     *                    Should be in sync with Jetty org.eclipse.jetty.server.Request.maxFormContentSize setting.
     *                    Production default is 200M, see runBlazegraph.sh file.
     *                    If that setting is changed, this one should change too, otherwise we get POST errors on big updates.
     *                    See: https://phabricator.wikimedia.org/T210235
     */
    public RdfRepository(UrisScheme uris, RdfClient rdfClient, long maxPostSize, UpdateMode updateMode) {
        this.uris = uris;
        this.rdfClient = rdfClient;

        msyncBody = loadBody("multiSync");
        updateLeftOffTimeBody = loadBody("updateLeftOffTime");
        getValues = loadBody("GetValues");
        getRefs = loadBody("GetRefs");
        cleanUnused = loadBody("CleanUnused");
        getRevisions = loadBody("GetRevisions");
        verifyExtra = loadBody("verifyExtra");
        verifyMissing = loadBody("verifyMissing");
        getLexemes = loadBody("GetLexemes");
        maxStatementsPerBatch = maxPostSize / 400;
        maxPostDataSize = (maxPostSize - 1024 * 1024) / 2;
        this.updateMode = updateMode;
    }

    /**
     * Loads some sparql.
     *
     * @param name name of the sparql file to load - the actual file loaded is
     *            RdfRepository.%name%.sparql.
     * @return contents of the sparql file
     * @throws FatalException if there is an error loading the file
     */
    private static String loadBody(String name) {
        return Utils.loadBody(name, RdfRepository.class);
    }

    /**
     * Collect results of the query into string set.
     *
     * @param result Result object
     * @param binding Binding name to collect
     * @return Collection of strings resulting from the query.
     */
    private Set<String> resultToSet(TupleQueryResult result, String binding) {
        HashSet<String> values = new HashSet<>();
        try {
            while (result.hasNext()) {
                Binding value = result.next().getBinding(binding);
                if (value == null) {
                    continue;
                }
                values.add(value.getValue().stringValue());
            }
        } catch (QueryEvaluationException e) {
            throw new FatalException("Can't load results: " + e, e);
        }
        return values;
    }


    /**
     * Get list of value subjects connected to entity. The connection is either
     * via statement or via reference or via qualifier.
     *
     * @return Set of value subjects
     */
    public ImmutableSetMultimap<String, String> getValues(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getValues);
        b.bindUris("entityList", entityIds);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return rdfClient.selectToMap(b.toString(), "entity", "s");
    }

    /**
     * Get list of reference subjects connected to entity.
     *
     * @return Set of references
     */
    public ImmutableSetMultimap<String, String> getRefs(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getRefs);
        b.bindUris("entityList", entityIds);
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return rdfClient.selectToMap(b.toString(), "entity", "s");
    }

    /**
     * Sync repository from changes list.
     *
     * Synchronizes the RDF repository's representation of an entity to be
     * exactly the provided statements. You can think of the RDF managed for an
     * entity as a tree rooted at the entity. The managed tree ends where the
     * next entity's managed tree starts. For example Q23 from wikidata includes
     * all statements about George Washington but not those about Martha
     * (Q191789) even though she is linked by the spouse attribute. On the other
     * hand the qualifiers on statements about George are included in George.
     *
     * @param changes List of changes.
     * @return Number of triples modified.
     */
    public int syncFromChanges(Collection<Change> changes, boolean verifyResult) {
        if (changes.isEmpty()) {
            // no changes, we're done
            return 0;
        }
        switch (updateMode) {
            case NON_MERGING:
                return syncFromChangesNonMerging(changes, verifyResult);
            case MERGING:
                return syncFromChangesMerging(changes, verifyResult);
            default:
                throw new IllegalStateException("Unknown update mode");
        }
    }
    /**
     * Sync repository from changes list using Update SPARQL.
     * See also {@link #syncFromChanges(Collection, boolean)}
     *
     * @param changes List of changes.
     * @return Number of triples modified.
     */
    private int syncFromChangesNonMerging(Collection<Change> changes, boolean verifyResult) {
        UpdateBuilder b = new UpdateBuilder(msyncBody);
        // Pre-bind static elements of the template
        b.bindUri("schema:about", SchemaDotOrg.ABOUT);
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindValue("ts", Instant.now());

        Set<String> entityIds = newHashSetWithExpectedSize(changes.size());
        List<Statement> insertStatements = new ArrayList<>();
        ClassifiedStatements classifiedStatements = new ClassifiedStatements(uris);

        Set<String> valueSet = new HashSet<>();
        Set<String> refSet = new HashSet<>();

        // Pre-filled query template to use in the batch loop below
        final String queryTemplate = b.toString();

        int modified = 0;
        for (final Change change : changes) {
            if (change.getStatements() == null) {
                // broken change, probably failed retrieval
                continue;
            }
            entityIds.add(change.entityId());
            insertStatements.addAll(change.getStatements());
            classifiedStatements.classify(change.getStatements(), change.entityId());
            valueSet.addAll(change.getValueCleanupList());
            refSet.addAll(change.getRefCleanupList());
            // If current batch data has grown too big, we send it out and start the new one.
            if (insertStatements.size() > maxStatementsPerBatch || classifiedStatements.getDataSize() > maxPostDataSize) {
                // Send the batch out and clean up
                // Logging as info for now because I want to know how many split batches we get. I don't want too many.
                log.info("Too much data with {} bytes - sending batch out, last ID {}", classifiedStatements.getDataSize(), change.entityId());

                modified += sendUpdateBatch(queryTemplate, entityIds, insertStatements, classifiedStatements,
                        refSet, valueSet, verifyResult);
                entityIds.clear();
                insertStatements.clear();
                classifiedStatements.clear();
                valueSet.clear();
                refSet.clear();
            }
        }

        if (!entityIds.isEmpty()) {
            modified += sendUpdateBatch(queryTemplate, entityIds,
                                        insertStatements, classifiedStatements,
                    refSet, valueSet, verifyResult);
        }

        return modified;
    }

    private int sendUpdateBatch(String queryTemplate,
                                Set<String> entityIds,
                                List<Statement> insertStatements,
                                ClassifiedStatements classifiedStatements,
                                Set<String> valueSet,
                                Set<String> refSet,
                                boolean verifyResult
    ) {
        log.debug("Processing {} IDs and {} statements", entityIds.size(), insertStatements.size());
        UpdateBuilder b = new UpdateBuilder(queryTemplate);
        b.bindEntityIds("entityListTop", entityIds, uris);
        logUpdatedEntityIds(entityIds);
        entityIds.addAll(fetchLexemeSubIds(entityIds));
        b.bindEntityIds("entityList", entityIds, uris);
        b.bindStatements("insertStatements", insertStatements);
        b.bindValues("entityStatements", classifiedStatements.entityStatements);

        b.bindValues("statementStatements", classifiedStatements.statementStatements);
        b.bindValues("aboutStatements", classifiedStatements.aboutStatements);
        b.bindValue("ts", Instant.now());

        if (!refSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", refSet);
            // This is not necessary but easier than having separate templates
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            b.bind("refCleanupQuery", cleanup.toString());
        }  else {
            b.bind("refCleanupQuery", "");
        }

        if (!valueSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", valueSet);
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            b.bind("valueCleanupQuery", cleanup.toString());
        }  else {
            b.bind("valueCleanupQuery", "");
        }

        long start = System.currentTimeMillis();
        log.debug("Sending query {} bytes", b.toString().length());
        Integer modified = rdfClient.update(b.toString());
        log.debug("Update query took {} millis and modified {} statements",
                System.currentTimeMillis() - start, modified);

        if (verifyResult) {
            try {
                verifyStatements(entityIds, insertStatements);
            } catch (QueryEvaluationException e) {
                throw new FatalException("Can't load verify results: " + e, e);
            }
        }

        return modified;
    }

    /**
     * Sync repository from changes list using MergingUpdaterServlet.
     * See also {@link #syncFromChanges(Collection, boolean)}
     * @param changes List of changes.
     * @return Number of triples modified.
     */
    private int syncFromChangesMerging(Collection<Change> changes, boolean verifyResult) {
        if (changes.isEmpty()) {
            // no changes, we're done
            return 0;
        }

        Set<String> entityIds = newHashSetWithExpectedSize(changes.size());
        List<Statement> insertStatements = new ArrayList<>();

        Set<String> valueSet = new HashSet<>();
        Set<String> refSet = new HashSet<>();

        int modified = 0;
        long statementsDataSize = 0;
        for (final Change change : changes) {
            if (change.getStatements() == null) {
                // broken change, probably failed retrieval
                continue;
            }
            entityIds.add(change.entityId());
            insertStatements.addAll(change.getStatements());
            valueSet.addAll(change.getValueCleanupList());
            refSet.addAll(change.getRefCleanupList());
            statementsDataSize += change.getStatements().stream().collect(Collectors.summarizingLong(st ->
                11 + // overhead per statement
                st.getSubject().stringValue().length() +
                st.getPredicate().stringValue().length() +
                st.getObject().stringValue().length()
            )).getSum();
            // If current batch data has grown too big, we send it out and start the new one.
            if (insertStatements.size() > maxStatementsPerBatch || statementsDataSize > maxPostDataSize) {
                // Send the batch out and clean up
                // Logging as info for now because I want to know how many split batches we get. I don't want too many.
                log.info("Too much data with {} bytes - sending batch out, last ID {}", statementsDataSize, change.entityId());

                modified += sendMergingUpdateBatch(entityIds, insertStatements, refSet, valueSet, verifyResult);
                entityIds.clear();
                insertStatements.clear();
                valueSet.clear();
                refSet.clear();
                statementsDataSize = 0;
            }
        }

        if (!entityIds.isEmpty()) {
            modified += sendMergingUpdateBatch(entityIds, insertStatements, refSet, valueSet, verifyResult);
        }

        return modified;
    }

    /**
     * Send a batch of updates to RDF database.
     * @return Number of triples modified in the update (from database)
     */
    private int sendMergingUpdateBatch(Set<String> entityIds,
                                List<Statement> insertStatements,
                                Set<String> valueSet,
                                Set<String> refSet,
                                boolean verifyResult
    ) {
        log.debug("Processing {} IDs and {} statements", entityIds.size(), insertStatements.size());
        logUpdatedEntityIds(entityIds);

        long start = System.currentTimeMillis();
        Integer modified = rdfClient.mergingUpdate(insertStatements, valueSet, refSet);
        log.debug("Update query took {} millis and modified {} statements",
                System.currentTimeMillis() - start, modified);

        if (verifyResult) {
            try {
                verifyStatements(entityIds, insertStatements);
            } catch (QueryEvaluationException e) {
                throw new FatalException("Can't verify update results: " + e, e);
            }
        }

        return modified;
    }

    private void logUpdatedEntityIds(Set<String> entityIds) {
        entityIds.forEach(entityId -> UPDATED_ENTITY_IDS_LOG.info(entityId));
    }

    /**
     * Fetch sub-ids for given lexeme entity IDs.
     * We need them because forms & senses have statements too.
     * @param entityIds Set of parent entity IDs.
     * @return List of IDs for forms and senses.
     */
    private List<String> fetchLexemeSubIds(Set<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getLexemes);
        b.bindEntityIds("entityList", entityIds, uris);
        return rdfClient.getEntityIds(b.toString(), "lex", uris);
    }

    /**
     * Verify that the database matches the statement data for these IDs.
     * @param entityIds List of IDs
     * @param statements List of statements for these IDs
     * @throws QueryEvaluationException if there is a problem retrieving result.
     */
    private void verifyStatements(Set<String> entityIds, List<Statement> statements)
            throws QueryEvaluationException {
        log.debug("Verifying the update - looking for extra data");
        UpdateBuilder bv = new UpdateBuilder(verifyExtra);
        bv.bindUri("schema:about", SchemaDotOrg.ABOUT)
            .bindUri("wikibase:timestamp", Ontology.TIMESTAMP)
            .bind("uris.statement", uris.statement())
            .bindEntityIds("entityList", entityIds, uris)
            .bindValues("allStatements", statements)
            .bindBnodeValues("bnodeStatements", statements);
        TupleQueryResult result = rdfClient.query(bv.toString());
        if (result.hasNext()) {
            log.error("Update failed, we have extra data!");
            printResultData(result);
            throw new FatalException("Update failed, bad old data in the store");
        }
        log.debug("Verifying the update - looking for missing data");
        bv = new UpdateBuilder(verifyMissing);
        bv.bindUri("schema:about", SchemaDotOrg.ABOUT)
                .bindUri("wikibase:timestamp", Ontology.TIMESTAMP)
                .bindUri("wikibase:wikiGroup", Ontology.WIKIGROUP)
                .bind("uris.statement", uris.statement())
                .bind("uris.value", uris.value())
                .bind("uris.reference", uris.reference())
                .bindEntityIds("entityList", entityIds, uris)
                .bindValues("allStatements", statements)
                .bindBnodeValues("bnodeStatements", statements);
        result = rdfClient.query(bv.toString());
        if (result.hasNext()) {
            log.error("Update failed, we have missing data!");
            printResultData(result);
            throw new FatalException("Update failed, data missing from the store");
        }
        log.debug("Verification OK");
    }

    @SuppressFBWarnings(value = "SLF4J_SIGN_ONLY_FORMAT", justification = "We rely on that format.")
    private void printResultData(TupleQueryResult result) throws QueryEvaluationException {
        while (result.hasNext()) {
            BindingSet bindings = result.next();
            Binding s = bindings.getBinding("s");
            Binding p = bindings.getBinding("p");
            Binding o = bindings.getBinding("o");
            log.error("{}\t{}\t{}", s.getValue().stringValue(),
                    p.getValue().stringValue(), o.getValue().stringValue());
        }
    }

    /**
     * Filter set of changes and see which of them really need to be updated.
     * The changes that have their revision or better in the repo do not need update.
     * @param candidates List of candidate changes
     * @return Set of entity IDs for which the update is needed.
     */
    public Set<String> hasRevisions(Collection<Change> candidates) {
        UpdateBuilder b = new UpdateBuilder(getRevisions);
        StringBuilder values = new StringBuilder();
        for (Change entry: candidates) {
            values.append("( <").append(uris.entityIdToURI(entry.entityId())).append("> ")
                    .append(entry.revision()).append(" )\n");
        }
        b.bind("values", values.toString());
        b.bindUri("schema:version", SchemaDotOrg.VERSION);
        return resultToSet(rdfClient.query(b.toString()), "s");
    }

    /**
     * Does the triple store have this revision or better.
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "we want to be platform independent here.")
    public boolean hasRevision(String entityId, long revision) {
        return rdfClient.ask(String.format(Locale.ROOT,
                "ASK {\n wd:%s schema:version ?v .\n  FILTER (?v >= %s)\n}",
                entityId, revision));
    }

    /**
     * Fetch where we left off updating the repository.
     *
     * @return the date or null if we have nowhere to start from
     */
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "prefix() is called with different StringBuilders")
    public Instant fetchLeftOffTime() {
        log.info("Checking for left off time from the updater");
        StringBuilder b = SchemaDotOrg.prefix(new StringBuilder());
        b.append("SELECT * WHERE { <").append(uris.root()).append("> schema:dateModified ?date }");
        Instant leftOffTime = dateFromQuery(b.toString());
        if (leftOffTime != null) {
            log.info("Found left off time from the updater");
            return leftOffTime;
        }
        log.info("Checking for left off time from the dump");
        b = Ontology.prefix(SchemaDotOrg.prefix(new StringBuilder()));
        // Only use the earliest TS from the dump since
        b.append("SELECT * WHERE { ontology:Dump schema:dateModified ?date } ORDER BY ASC(?date) LIMIT 1");
        return dateFromQuery(b.toString());
    }

    /**
     * Update where we left off so when fetchLeftOffTime is next called it
     * returns leftOffTime so we can continue from there after the updater is
     * restarted.
     */
    public void updateLeftOffTime(Instant leftOffTime) {
        log.debug("Setting last updated time to {}", leftOffTime);
        UpdateBuilder b = new UpdateBuilder(updateLeftOffTimeBody);
        b.bindUri("root", uris.root());
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        b.bindValue("date", leftOffTime);
        rdfClient.update(b.toString());
    }

    /**
     * Run a query that returns just a date in the "date" binding and return its
     * result.
     */
    private Instant dateFromQuery(String query) {
        TupleQueryResult result = rdfClient.query(query);
        try {
            if (!result.hasNext()) {
                return null;
            }
            Binding maxLastUpdate = result.next().getBinding("date");
            if (maxLastUpdate == null) {
                return null;
            }
            // Note that XML calendar and Instant have the same default format
            XMLGregorianCalendar xmlCalendar = ((Literal) maxLastUpdate.getValue()).calendarValue();
            /*
             * We convert rather blindly to a GregorianCalendar because we're
             * reasonably sure all the right data is present.
             */
            GregorianCalendar calendar = xmlCalendar.toGregorianCalendar();
            return calendar.getTime().toInstant();
        } catch (QueryEvaluationException e) {
            throw new FatalException("Error evaluating query", e);
        }
    }

    public enum UpdateMode {
        MERGING, NON_MERGING
    }

    /**
     * Detects the values that are no longer referenced from the entity statements.
     * @param existingValues the existing values as known by the rdf repository (old state)
     * @param entityStatements the statements of the entity (new state before munging)
     * @return A list of potential orphaned values
     */
    public static Set<String> extractValuesToCleanup(Collection<String> existingValues, Collection<Statement> entityStatements) {
        return extractSubjectsToCleanup(existingValues, entityStatements, StatementPredicates::valueTypeStatement);
    }

    /**
     * Detects the referenced that are no longer referenced from the entity statements.
     * @param existingReferences the existing referenced as known by the rdf repository (old state)
     * @param entityStatements the statements of the entity (new state before munging)
     * @return A list of potential orphaned references
     */
    public static Set<String> extractReferencesToCleanup(Collection<String> existingReferences, Collection<Statement> entityStatements) {
        return extractSubjectsToCleanup(existingReferences, entityStatements, StatementPredicates::referenceTypeStatement);
    }

    private static Set<String> extractSubjectsToCleanup(Collection<String> existingSubjects, Collection<Statement> entityStatements,
                                                        Predicate<Statement> subjectFilter) {
        if (existingSubjects.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> entitySubjects = entityStatements.stream().filter(subjectFilter).map(s -> s.getSubject().stringValue()).collect(toSet());
        return existingSubjects.stream().filter(s -> !entitySubjects.contains(s)).collect(toSet());
    }
}

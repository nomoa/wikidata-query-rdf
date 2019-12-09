package org.wikidata.query.rdf.tool.rdf;

import static org.wikidata.query.rdf.common.uri.PropertyType.REFERENCE_VALUE;
import static org.wikidata.query.rdf.common.uri.PropertyType.REFERENCE_VALUE_NORMALIZED;

import java.util.function.Predicate;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Ontology.Geo;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.Ontology.Time;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;

public final class StatementPredicates {
    private StatementPredicates() {}
    /**
     * Detects a triple using schema:softwareVersion as a predicate.
     */
    public static boolean softwareVersion(Statement statement) {
        return statement.getPredicate().stringValue().equals(SchemaDotOrg.SOFTWARE_VERSION);
    }

    /**
     * all statements with a wikibase:Dump subject.
     */
    public static boolean dumpStatement(Statement statement) {
        return statement.getSubject().stringValue().equals(Ontology.DUMP);
    }

    /**
     * Statement describing the format version of the dump.
     * <tt>wikibase:Dump schema:softwareVersion "1.0.0" </tt>
     */
    public static boolean dumpFormatVersion(Statement statement) {
        return dumpStatement(statement) && softwareVersion(statement);
    }

    /**
     * Statement reflecting a redirect.
     * <tt>wd:Q6825 owl:sameAs wd:Q26709563</tt>
     */
    public static boolean redirect(Statement statement) {
        // should we make sure that the subject and the object belong to the entity ns?
        return statement.getPredicate().stringValue().equals(OWL.SAME_AS);
    }

    /**
     * A statement declaring a type.
     * e.g.
     * subject a object
     */
    public static boolean typeStatement(Statement statement) {
        return RDF.TYPE.equals(statement.getPredicate().stringValue());
    }

    /**
     * Statement whose subject is in the ns namespace.
     */
    public static Predicate<Statement> subjectInNamespace(String ns) {
        return stmt -> inNamespace(stmt.getSubject().stringValue(), ns);
    }

    /**
     * Statement whose object is in the ns namespace.
     */
    public static Predicate<Statement> objectInNamespace(String ns) {
        return stmt -> inNamespace(stmt.getObject().stringValue(), ns);
    }

    /**
     * Statement whose predicate is in the ns namespace.
     */
    public static Predicate<Statement> predicateInNamespace(String ns) {
        return stmt -> inNamespace(stmt.getPredicate().stringValue(), ns);
    }

    private static boolean inNamespace(String uri, String namespace) {
        return uri.startsWith(namespace) && uri.indexOf('/', namespace.length()) < 0;
    }

    /**
     * Statement declaring a reference.
     * e.g.
     * ref:UUID a wikibase:Reference
     */
    public static boolean referenceTypeStatement(Statement statement) {
        return typeStatement(statement) && statement.getObject().stringValue().equals(Ontology.REFERENCE);
    }

    /**
     * Statement declaring a value.
     * e.g.
     * val:UUID a wikibase:QuantityValue
     */
    public static boolean valueTypeStatement(Statement statement) {
        String object = statement.getObject().stringValue();
        return typeStatement(statement) &&
                (object.equals(Quantity.TYPE) || object.equals(Time.TYPE) || object.equals(Geo.TYPE));
    }

    /**
     * Whether this triple links reference and value.
     * - subject is a reference
     * - predicate must be prv:XYZ or prn:XYZ
     * - object must be a value
     * @return Is it a ref to value link?
     */
    public static Predicate<Statement> tripleRefValue(UrisScheme uris) {
        // ref:XYZ Prop:RefValue|Prop:RefValueNormalized value:
        return subjectInNamespace(uris.reference()).and(
                objectInNamespace(uris.value()).and(
                predicateInNamespace(uris.property(REFERENCE_VALUE))
                        .or(predicateInNamespace(uris.property(REFERENCE_VALUE_NORMALIZED)))));
    }

    /**
     * Whether the triple links the entity to a reified statement.
     * - subject is the entityUri provided
     * - predicate belongs to the {@link org.wikidata.query.rdf.common.uri.PropertyType#CLAIM} namespace
     * - object is in the {@link UrisScheme#statement()} namespace
     * e.g.
     *     wd:Q2 p:P1419 s:Q2-e055dd8f-4e6f-36ea-819d-5c4de88b57a0 .
     */
    public static Predicate<Statement> reificationStatement(UrisScheme uris) {
        return objectInNamespace(uris.statement())
                .and(predicateInNamespace(uris.property(PropertyType.CLAIM)));
    }
}

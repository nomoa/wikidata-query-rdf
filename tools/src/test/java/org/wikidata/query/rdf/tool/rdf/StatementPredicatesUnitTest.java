package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.function.Predicate;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;

public class StatementPredicatesUnitTest {

    @Test
    public void softwareVersion() {
        assertThat(StatementPredicates.softwareVersion(statement("uri:foo", SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.softwareVersion(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void dumpStatement() {
        assertThat(StatementPredicates.dumpStatement(statement(Ontology.DUMP, "uri:bar", new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.dumpStatement(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void dumpFormatVersion() {
        assertThat(StatementPredicates.dumpFormatVersion(statement(Ontology.DUMP, SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.dumpFormatVersion(statement(Ontology.DUMP, "uri:bar", new LiteralImpl("bar")))).isFalse();
        assertThat(StatementPredicates.dumpFormatVersion(statement("uri:foo", SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void redirect() {
        assertThat(StatementPredicates.redirect(statement("uri:foo", OWL.SAME_AS, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.redirect(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void typeStatement() {
        assertThat(StatementPredicates.typeStatement(statement("uri:foo", RDF.TYPE, "uri:footype"))).isTrue();
        assertThat(StatementPredicates.typeStatement(statement("uri:foo", "uri:bartype", "uri:footype"))).isFalse();
    }

    @Test
    public void subjectInNamespace() {
        assertThat(StatementPredicates.subjectInNamespace("uri:foo/").test(statement("uri:foo/en", "uri:foopred", "uri:bar/obj"))).isTrue();
        assertThat(StatementPredicates.subjectInNamespace("uri:foo/").test(statement("uri:fo/en", "uri:foopred", "uri:bar/obj"))).isFalse();
    }

    @Test
    public void objectInNamespace() {
        assertThat(StatementPredicates.objectInNamespace("uri:bar/").test(statement("uri:foo/en", "uri:foopred", "uri:bar/obj"))).isTrue();
        assertThat(StatementPredicates.objectInNamespace("uri:bar/").test(statement("uri:fo/en", "uri:foopred", "uri:ba/obj"))).isFalse();
    }

    @Test
    public void predicateInNamespace() {
        assertThat(StatementPredicates.predicateInNamespace("uri:foopred/").test(statement("uri:foo/en", "uri:foopred/en", "uri:bar/obj"))).isTrue();
        assertThat(StatementPredicates.predicateInNamespace("uri:foopred/").test(statement("uri:fo/en", "uri:fopred/en", "uri:ba/obj"))).isFalse();
    }

    @Test
    public void referenceTypeStatement() {
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", RDF.TYPE, Ontology.REFERENCE))).isTrue();
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", RDF.TYPE, "uri:foo"))).isFalse();
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", "uri:foo", Ontology.REFERENCE))).isFalse();
    }

    @Test
    public void valueTypeStatement() {
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Time.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Geo.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Quantity.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, "uri:foo"))).isFalse();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", "uri:foo", Ontology.Time.TYPE))).isFalse();
    }

    @Test
    public void tripleRefValue() {
        UrisScheme scheme = mock(UrisScheme.class);
        when(scheme.property(PropertyType.REFERENCE_VALUE)).thenReturn("prop:refval/");
        when(scheme.property(PropertyType.REFERENCE_VALUE_NORMALIZED)).thenReturn("prop:refvalnorm/");
        when(scheme.value()).thenReturn("val:value/");
        when(scheme.reference()).thenReturn("ref:reference/");

        Predicate<Statement> refToVal = StatementPredicates.tripleRefValue(scheme);
        assertThat(refToVal.test(statement("ref:reference/ref-id", "prop:refval/P12", "val:value/val-id"))).isTrue();
        assertThat(refToVal.test(statement("ref:reference/ref-id", "prop:refvalnorm/P12", "val:value/val-id"))).isTrue();
        assertThat(refToVal.test(statement("usr:foo", "prop:refval/P12", "val:value/val-id"))).isFalse();
        assertThat(refToVal.test(statement("ref:reference/ref-id", "uri:foo", "val:value/val-id"))).isFalse();
        assertThat(refToVal.test(statement("ref:reference/ref-id", "prop:refval/P12", "uri:foo"))).isFalse();
    }

    @Test
    public void reificationStatement() {
        UrisScheme scheme = mock(UrisScheme.class);
        when(scheme.statement()).thenReturn("stmt:statement/");
        when(scheme.property(PropertyType.CLAIM)).thenReturn("prop:claim/");
        Predicate<Statement> reificationStmt = StatementPredicates.reificationStatement(scheme);
        assertThat(reificationStmt.test(statement("uri:entity/123", "prop:claim/P12", "stmt:statement/ST-ID"))).isTrue();
        assertThat(reificationStmt.test(statement("uri:foo", "prop:claim/P12", "stmt:statement/ST-ID"))).isTrue();
        assertThat(reificationStmt.test(statement("uri:entity/123", "prop:claim/P12", "uri:foo"))).isFalse();
        assertThat(reificationStmt.test(statement("uri:entity/123", "uri:foo", "stmt:statement/ST-ID"))).isFalse();
    }
}

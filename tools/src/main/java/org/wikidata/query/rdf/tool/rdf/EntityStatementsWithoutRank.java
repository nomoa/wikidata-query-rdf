package org.wikidata.query.rdf.tool.rdf;

import static java.util.Collections.singleton;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;

/**
 * Collect all wikibase statements that have no rank.
 * For context see https://phabricator.wikimedia.org/T203646#4572841
 * This code may not be useful anymore.
 */
public class EntityStatementsWithoutRank implements Collector<Statement, EntityStatementsWithoutRank.StatementsAndRanks, Set<String>> {
    private static final Set<Characteristics> CHARACTERISTICS = singleton(Characteristics.UNORDERED);
    private static final EntityStatementsWithoutRank COLLECTOR = new EntityStatementsWithoutRank();

    public static EntityStatementsWithoutRank entityStatementsWithoutRank() {
        return COLLECTOR;
    }

    @Override
    public Supplier<StatementsAndRanks> supplier() {
        return StatementsAndRanks::new;
    }

    @Override
    public BiConsumer<StatementsAndRanks, Statement> accumulator() {
        return (accum, statement) -> {
            if (statement.getObject().stringValue().equals(Ontology.STATEMENT) && RDF.TYPE.equals(statement.getPredicate().stringValue())) {
                accum.statements.add(statement.getSubject().stringValue());
            } else if (statement.getPredicate().stringValue().equals(Ontology.RANK)) {
                accum.subjectsWithRank.add(statement.getSubject().stringValue());
            }
        };
    }

    @Override
    public BinaryOperator<StatementsAndRanks> combiner() {
        return (a, b) -> {
            a.subjectsWithRank.addAll(b.subjectsWithRank);
            a.statements.addAll(b.statements);
            return a;
        };
    }

    @Override
    public Function<StatementsAndRanks, Set<String>> finisher() {
        return ranks -> {
            ranks.statements.removeAll(ranks.subjectsWithRank);
            return ranks.statements;
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return CHARACTERISTICS;
    }

    static class StatementsAndRanks {
        /**
         * Id of the statements as in "wikibase statement" (type: {@link Ontology#STATEMENT}).
         */
        private final Set<String> statements = new HashSet<>();

        /**
         * All subjects that have a {@link Ontology#RANK} predicate.
         */
        private final Set<String> subjectsWithRank = new HashSet<>();
    }
}

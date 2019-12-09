package org.wikidata.query.rdf.common.uri;

/**
 * Marks the kinds of things (items or properties).
 * These are matching wikibase: prefix.
 */
public final class Ontology {
    /**
     * Common prefix of all ontology parts.
     */
    public static final String NAMESPACE = "http://wikiba.se/ontology#";

    /**
     * Wikibase exports all items with an assertion that their RDF.TYPE is this
     * and we filter that out. Its also used as a inline literal type for
     * inlining uris in Blazegraph.
     */
    public static final String ITEM = NAMESPACE + "Item";
    /**
     * Wikibase exports all items with an assertion that their RDF.TYPE is this.
     * Its also used as a inline literal type for inlining uris in Blazegraph.
     */
    public static final String PROPERTY = NAMESPACE + "Property";
    /**
     * Wikibase exports all statements with an assertion that their RDF.TYPE is
     * this and we filter that out.
     */
    public static final String STATEMENT = NAMESPACE + "Statement";
    /**
     * Wikibase exports references with an assertion that their RDF.TYPE is this
     * and we filter that out.
     */
    public static final String REFERENCE = NAMESPACE + "Reference";
    /**
     * Wikibase exports values with an assertion that their RDF.TYPE is this and
     * we filter that out.
     * Mentioned in the wikibase ontology as a super type for all 3 types of value.
     * Should not be used directly here unless we start doing some reasoning.
     */
    public static final String VALUE = NAMESPACE + "Value";

    /**
     * Wikibase exports dump information with this subject.
     */
    public static final String DUMP = NAMESPACE + "Dump";

    /**
     * Predicate for marking Wikibase's Rank.
     *
     * @see <a href="http://www.wikidata.org/wiki/Help:Ranking">The
     *      documentation for ranking</a>
     */
    public static final String RANK = NAMESPACE + "rank";
    /**
     * Statements with the best rank. These are the ones you usually want to
     * find.
     */
    public static final String BEST_RANK = NAMESPACE + "BestRank";
    /**
     * Rank that overrides all normal rank statements.
     */
    public static final String PREFERRED_RANK = NAMESPACE + "PreferredRank";
    /**
     * Rank that is best if there are no preferred rank statements.
     */
    public static final String NORMAL_RANK = NAMESPACE + "NormalRank";
    /**
     * Rank that isn't even considered best when there are no better statement.
     * Its so bad you need to explicitly ask for things of this rank to get
     * them.
     */
    public static final String DEPRECATED_RANK = NAMESPACE + "DeprecatedRank";
    /**
     * Prefix for the label service. Not used in that data - just for SPARQL
     * queries.
     */
    public static final String LABEL = NAMESPACE + "label";
    /**
     * Prefix for wiki group.
     */
    public static final String WIKIGROUP = NAMESPACE + "wikiGroup";
    /**
     * Badge sitelink property.
     */
    public static final String BADGE = NAMESPACE + "badge";
    /**
     * Statements count.
     */
    public static final String STATEMENTS = NAMESPACE + "statements";
    /**
     * Sitelinks count.
     */
    public static final String SITELINKS = NAMESPACE + "sitelinks";
    /**
     * Sitelinks count.
     */
    public static final String IDENTIFIERS = NAMESPACE + "identifiers";
    /**
     * Constraint violation.
     */
    public static final String CONSTRAINT_VIOLATION = NAMESPACE + "hasViolationForConstraint";
    /**
     * Timestamp for marking when entity was loaded.
     */
    public static final String TIMESTAMP = NAMESPACE + "timestamp";

    /**
     * Predicates and objects to describe WikibaseLexeme items.
     */
    public static final class Lexeme {
        /**
         * Lexeme class.
         */
        public static final String LEXEME = NAMESPACE + "Lexeme";
        /**
         * Form class.
         */
        public static final String FORM = NAMESPACE + "Form";
        /**
         * Sense class.
         */
        public static final String SENSE = NAMESPACE + "Sense";
        /**
         * Lemma predicate.
         */
        public static final String LEMMA = NAMESPACE + "lemma";
        /**
         * Lexical category predicate.
         */
        public static final String LEXICAL_CATEGORY = NAMESPACE + "lexicalCategory";
        /**
         * Grammatical feature predicate.
         */
        public static final String GRAMMATICAL_FEATURE = NAMESPACE + "grammaticalFeature";

        private Lexeme() {
            // Utility class.
        }
    }

    /**
     * Predicates used to describe a time.
     */
    public static final class Time {
        /**
         * wikibase:TimeValue type.
         */
        public static final String TYPE = NAMESPACE + "TimeValue";
        /**
         * Common prefix of all time predicates.
         */
        private static final String PREFIX = NAMESPACE + "time";
        /**
         * The actual value of the time. We will always load this value exactly
         * as wikibase exports it - never normalize it for precision, timezone,
         * or calendar model.
         */
        public static final String VALUE = PREFIX + "Value";
        /**
         * The precision of the time. Wikibase exports integers with specific
         * meanings: 0 - billion years, 1 - hundred million years, ..., 6 -
         * millennium, 7 - century, 8 - decade, 9 - year, 10 - month, 11 - day,
         * 12 - hour, 13 - minute, 14 - second.
         */
        public static final String PRECISION = PREFIX + "Precision";
        /**
         * Timezone in which the time was originally defined. A signed integer
         * representing offset from UTC in minutes.
         */
        public static final String TIMEZONE = PREFIX + "Timezone";
        // Wikibase exports are all UTC so this is only for GUI purposes
        /**
         * Calendar model in which the date was defined.
         */
        public static final String CALENDAR_MODEL = PREFIX + "CalendarModel";
        // Wikibase exports are all Gregorian so this is only for GUI purposes
        private Time() {
            // Utility class.
        }
    }

    /**
     * Predicates used to describe a geographic point.
     */
    public static final class Geo {
        /**
         * wikibase:GlobecoordinateValue type.
         */
        public static final String TYPE = NAMESPACE + "GlobecoordinateValue";
        /**
         * Common prefix of all geo predicates.
         */
        private static final String PREFIX = NAMESPACE + "geo";

        /**
         * The latitude part of the point.
         */
        public static final String LATITUDE = PREFIX + "Latitude";
        /**
         * The longitude part of the point.
         */
        public static final String LONGITUDE = PREFIX + "Longitude";
        /**
         * The precision of the point.
         */
        public static final String PRECISION = PREFIX + "Precision";
        /**
         * The globe that the point is on.
         */
        public static final String GLOBE = PREFIX + "Globe";

        // TODO a better description for precision

        private Geo() {
            // Utility class.
        }
    }

    /**
     * Predicates used to describe a quantity.
     */
    public static final class Quantity {
        /**
         * wikibase:QuantityValue type.
         */
        public static final String TYPE = NAMESPACE + "QuantityValue";
        /**
         * Common prefix of all quantity predicates.
         */
        private static final String PREFIX = NAMESPACE + "quantity";

        /**
         * The number part of the quantity.
         */
        public static final String AMOUNT = PREFIX + "Amount";
        /**
         * The upper bound of the number part of the quantity.
         */
        public static final String UPPER_BOUND = PREFIX + "UpperBound";
        /**
         * The lower bound of the number part of the quantity.
         */
        public static final String LOWER_BOUND = PREFIX + "LowerBound";
        /**
         * The unit of the quantity.
         */
        public static final String UNIT = PREFIX + "Unit";
        /**
         * Normalized value for quantity.
         */
        public static final String NORMALIZED = PREFIX + "Normalized";

        private Quantity() {
            // Utility class.
        }
    }

    /**
     * Add prefix to a query.
     */
    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX ontology: <").append(NAMESPACE).append(">\n");
    }

    private Ontology() {
        // Utility class.
    }
}

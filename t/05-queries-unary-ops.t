#!perl

use strict;
use warnings;

use Test::More;
use Test::Differences;
use Test::Exception;
use ElasticSearch::SearchBuilder;

my $a = ElasticSearch::SearchBuilder->new;

test_queries(
    'UNARY OPERATOR: ids, not_ids',
    'IDS: 1',
    { -ids => 1 },
    { ids => { values => [1] } },

    'IDS: [1]',
    { -ids => [1] },
    { ids => { values => [1] } },

    'IDS: {V:1,T:foo}',
    { -ids => { values => 1,     type   => 'foo' } },
    { ids  => { type   => 'foo', values => [1] } },

    'IDS: {V:[1],T:[foo]}',
    { -ids => { values => [1],     type   => ['foo'], boost => 2 } },
    { ids  => { type   => ['foo'], values => [1],     boost => 2 } },

    'NOT_IDS: 1',
    { -not_ids => 1 },
    { bool => { must_not => [ { ids => { values => [1] } } ] } },

    'NOT_IDS: [1]',
    { -not_ids => [1] },
    { bool => { must_not => [ { ids => { values => [1] } } ] } },

    'NOT_IDS: {V:1,T:foo}',
    { -not_ids => { values => 1, type => 'foo' } },
    {   bool =>
            { must_not => [ { ids => { type => 'foo', values => [1] } } ] }
    },

    'NOT_IDS: {V:[1],T:[foo]}',
    { -not_ids => { values => [1], type => ['foo'], boost => 2 } },
    {   bool => {
            must_not =>
                [ { ids => { type => ['foo'], values => [1], boost => 2 } } ]
        }
    },

);

test_queries(
    'UNARY OPERATOR: flt, not_flt',
    'FLT: V',
    { -flt => 'v' },
    { flt => { like_text => 'v' } },

    'FLT: UNDEF',
    { -flt => undef },
    qr/HASHREF, SCALAR/,

    'FLT: [V]',
    { -flt => ['v'] },
    qr/HASHREF, SCALAR/,

    'FLT: {}',
    {   -flt => {
            like_text       => 'v',
            boost           => 1,
            fields          => [ 'foo', 'bar' ],
            ignore_tf       => 0,
            max_query_terms => 100,
            min_similarity  => 0.5,
            prefix_length   => 2
        }
    },
    {   flt => {
            like_text       => 'v',
            boost           => 1,
            fields          => [ 'foo', 'bar' ],
            ignore_tf       => 0,
            max_query_terms => 100,
            min_similarity  => 0.5,
            prefix_length   => 2
        }
    },

    'NOT_FLT: V',
    { -not_flt => 'v' },
    { bool => { must_not => [ { flt => { like_text => 'v' } } ] } },

    'NOT_FLT: UNDEF',
    { -not_flt => undef },
    qr/HASHREF, SCALAR/,

    'NOT_FLT: [V]',
    { -not_flt => ['v'] },
    qr/HASHREF, SCALAR/,

    'NOT_FLT: {}',
    {   -not_flt => {
            like_text       => 'v',
            boost           => 1,
            fields          => [ 'foo', 'bar' ],
            ignore_tf       => 0,
            max_query_terms => 100,
            min_similarity  => 0.5,
            prefix_length   => 2
        }
    },
    {   bool => {
            must_not => [ {
                    flt => {
                        like_text       => 'v',
                        boost           => 1,
                        fields          => [ 'foo', 'bar' ],
                        ignore_tf       => 0,
                        max_query_terms => 100,
                        min_similarity  => 0.5,
                        prefix_length   => 2
                    }
                }
            ]
        }
    },
);

test_queries(
    'UNARY OPERATOR: mlt, not_mlt',
    'MLT: V',
    { -mlt => 'v' },
    { mlt => { like_text => 'v' } },

    'MLT: UNDEF',
    { -mlt => undef },
    qr/HASHREF, SCALAR/,

    'MLT: [V]',
    { -mlt => ['v'] },
    qr/HASHREF, SCALAR/,

    'MLT: {}',
    {   -mlt => {
            like_text              => 'v',
            boost                  => 1,
            boost_terms            => 1,
            fields                 => [ 'foo', 'bar' ],
            max_doc_freq           => 100,
            max_query_terms        => 20,
            max_word_len           => 10,
            min_doc_freq           => 1,
            min_term_freq          => 1,
            min_word_len           => 1,
            percent_terms_to_match => 0.3,
            stop_words             => [ 'foo', 'bar' ]
        }
    },
    {   mlt => {
            like_text              => 'v',
            boost                  => 1,
            boost_terms            => 1,
            fields                 => [ 'foo', 'bar' ],
            max_doc_freq           => 100,
            max_query_terms        => 20,
            max_word_len           => 10,
            min_doc_freq           => 1,
            min_term_freq          => 1,
            min_word_len           => 1,
            percent_terms_to_match => 0.3,
            stop_words             => [ 'foo', 'bar' ]
        }
    },

    'NOT_MLT: V',
    { -not_mlt => 'v' },
    { bool => { must_not => [ { mlt => { like_text => 'v' } } ] } },

    'NOT_MLT: UNDEF',
    { -not_mlt => undef },
    qr/HASHREF, SCALAR/,

    'NOT_MLT: [V]',
    { -not_mlt => ['v'] },
    qr/HASHREF, SCALAR/,

    'NOT_MLT: {}',
    {   -not_mlt => {
            like_text              => 'v',
            boost                  => 1,
            boost_terms            => 1,
            fields                 => [ 'foo', 'bar' ],
            max_doc_freq           => 100,
            max_query_terms        => 20,
            max_word_len           => 10,
            min_doc_freq           => 1,
            min_term_freq          => 1,
            min_word_len           => 1,
            percent_terms_to_match => 0.3,
            stop_words             => [ 'foo', 'bar' ]
        }
    },
    {   bool => {
            must_not => [ {
                    mlt => {

                        like_text              => 'v',
                        boost                  => 1,
                        boost_terms            => 1,
                        fields                 => [ 'foo', 'bar' ],
                        max_doc_freq           => 100,
                        max_query_terms        => 20,
                        max_word_len           => 10,
                        min_doc_freq           => 1,
                        min_term_freq          => 1,
                        min_word_len           => 1,
                        percent_terms_to_match => 0.3,
                        stop_words             => [ 'foo', 'bar' ]
                    }
                }
            ]
        }
    },
);

for my $op (qw(-qs -query_string)) {
    test_queries(
        "UNARY OPERATOR: $op",

        "$op: V",
        { $op => 'v' },
        { query_string => { query => 'v' } },

        "$op: UNDEF",
        { $op => undef },
        qr/HASHREF, SCALAR/,

        "$op: [V]",
        { $op => ['v'] },
        qr/HASHREF, SCALAR/,

        "$op: {}",
        {   $op => {
                query                        => 'v',
                allow_leading_wildcard       => 0,
                analyzer                     => 'default',
                analyze_wildcard             => 1,
                auto_generate_phrase_queries => 0,
                boost                        => 1,
                default_operator             => 'AND',
                enable_position_increments   => 1,
                fields                       => [ 'foo', 'bar' ],
                fuzzy_min_sim                => 0.5,
                fuzzy_prefix_length          => 2,
                lowercase_expanded_terms     => 1,
                phrase_slop                  => 10,
                tie_breaker                  => 1.5,
                use_dis_max                  => 1
            }
        },
        {   query_string => {
                query                        => 'v',
                allow_leading_wildcard       => 0,
                analyzer                     => 'default',
                analyze_wildcard             => 1,
                auto_generate_phrase_queries => 0,
                boost                        => 1,
                default_operator             => 'AND',
                enable_position_increments   => 1,
                fields                       => [ 'foo', 'bar' ],
                fuzzy_min_sim                => 0.5,
                fuzzy_prefix_length          => 2,
                lowercase_expanded_terms     => 1,
                phrase_slop                  => 10,
                tie_breaker                  => 1.5,
                use_dis_max                  => 1
            }
        },
    );
}

for my $op (qw(-not_qs -not_query_string)) {
    test_queries(
        "UNARY OPERATOR: $op",

        "$op: V",
        { $op => 'v' },
        { bool => { must_not => [ { query_string => { query => 'v' } } ] } },

        "$op: UNDEF",
        { $op => undef },
        qr/HASHREF, SCALAR/,

        "$op: [V]",
        { $op => ['v'] },
        qr/HASHREF, SCALAR/,

        "$op: {}",
        {   $op => {
                query                        => 'v',
                allow_leading_wildcard       => 0,
                analyzer                     => 'default',
                analyze_wildcard             => 1,
                auto_generate_phrase_queries => 0,
                boost                        => 1,
                default_operator             => 'AND',
                enable_position_increments   => 1,
                fields                       => [ 'foo', 'bar' ],
                fuzzy_min_sim                => 0.5,
                fuzzy_prefix_length          => 2,
                lowercase_expanded_terms     => 1,
                phrase_slop                  => 10,
                tie_breaker                  => 1.5,
                use_dis_max                  => 1
            }
        },
        {   bool => {
                must_not => [ {
                        query_string => {
                            query                        => 'v',
                            allow_leading_wildcard       => 0,
                            analyzer                     => 'default',
                            analyze_wildcard             => 1,
                            auto_generate_phrase_queries => 0,
                            boost                        => 1,
                            default_operator             => 'AND',
                            enable_position_increments   => 1,
                            fields                       => [ 'foo', 'bar' ],
                            fuzzy_min_sim                => 0.5,
                            fuzzy_prefix_length          => 2,
                            lowercase_expanded_terms     => 1,
                            phrase_slop                  => 10,
                            tie_breaker                  => 1.5,
                            use_dis_max                  => 1
                        }
                    }
                ]
            }
        },
    );
}

test_queries(
    'UNARY OPERATOR: -bool',

    'bool: V',
    { -bool => 'v' },
    qr/HASHREF/,

    'bool: {}',
    {   -bool => {
            must                        => { k => 'v' },
            must_not                    => { k => 'v' },
            should                      => { k => 'v' },
            minimum_number_should_match => 1,
            disable_coord               => 1,
            boost                       => 2,
        }
    },
    {   bool => {
            must                        => [ { text => { k => 'v' } } ],
            must_not                    => [ { text => { k => 'v' } } ],
            should                      => [ { text => { k => 'v' } } ],
            minimum_number_should_match => 1,
            disable_coord               => 1,
            boost                       => 2,
        }
    },

    'bool: {[]}',
    {   -bool => {
            must     => [ { k => 'v' }, { k => 'v' } ],
            must_not => [ { k => 'v' }, { k => 'v' } ],
            should   => [ { k => 'v' }, { k => 'v' } ]
        }
    },
    {   bool => {
            must => [ { text => { k => 'v' } }, { text => { k => 'v' } } ],
            must_not =>
                [ { text => { k => 'v' } }, { text => { k => 'v' } } ],
            should => [ { text => { k => 'v' } }, { text => { k => 'v' } } ]
        }
    },

    'bool: {[empty]}',
    { -bool => { must => [], must_not => undef, should => 'foo' } },
    { bool => { should => [ { text => { _all => 'foo' } } ] } },

    'not_bool: {}',
    {   -not_bool => {
            must     => [ { k => 'v' }, { k => 'v' } ],
            must_not => [ { k => 'v' }, { k => 'v' } ],
            should   => [ { k => 'v' }, { k => 'v' } ]
        }
    },
    {   bool => {
            must_not => [ {
                    bool => {
                        must => [
                            { text => { k => 'v' } },
                            { text => { k => 'v' } }
                        ],
                        must_not => [
                            { text => { k => 'v' } },
                            { text => { k => 'v' } }
                        ],
                        should => [
                            { text => { k => 'v' } },
                            { text => { k => 'v' } }
                        ]
                    }
                }
            ]
        }
    },
);

test_queries(
    'UNARY OPERATOR: -boosting',

    'boosting: v',
    { -boosting => 'v' },
    qr/HASHREF/,

    'boosting: {}',
    {   -boosting => {
            positive       => { k => 'v' },
            negative       => { k => 'v' },
            negative_boost => 1
        }
    },
    {   boosting => {
            positive       => { text => { k => 'v' } },
            negative       => { text => { k => 'v' } },
            negative_boost => 1
        }
    },

    'boosting: {[]}',
    {   -boosting => {
            positive       => [ { k => 'v' }, { k => 'v' } ],
            negative       => [ { k => 'v' }, { k => 'v' } ],
            negative_boost => 1
        }
    },
    {   boosting => {
            positive => {
                bool => {
                    should => [
                        { text => { k => 'v' } }, { text => { k => 'v' } }
                    ]
                }
            },
            negative => {
                bool => {
                    should => [
                        { text => { k => 'v' } }, { text => { k => 'v' } }
                    ]
                }
            },
            negative_boost => 1
        }
    },

    'not_boosting: {[]}',
    {   -not_boosting => {
            positive       => [ { k => 'v' }, { k => 'v' } ],
            negative       => [ { k => 'v' }, { k => 'v' } ],
            negative_boost => 1
        }
    },
    {   bool => {
            must_not => [ {
                    boosting => {
                        positive => {
                            bool => {
                                should => [
                                    { text => { k => 'v' } },
                                    { text => { k => 'v' } }
                                ]
                            }
                        },
                        negative => {
                            bool => {
                                should => [
                                    { text => { k => 'v' } },
                                    { text => { k => 'v' } }
                                ]
                            }
                        },
                        negative_boost => 1
                    }
                },
            ]
        }
    }

);

for my $op (qw(-dis_max -dismax)) {
    test_queries(
        "UNARY OPERATOR: $op",

        "$op: V",
        { $op => 'v' },
        qr/ARRAYREF, HASHREF/,

        "$op: []",
        { $op => [ { k => 'v' }, { k => 'v' } ] },
        {   dis_max => {
                queries =>
                    [ { text => { k => 'v' } }, { text => { k => 'v' } } ]
            }
        },

        "$op: {}",
        {   $op => {
                queries     => [ { k => 'v' }, { k => 'v' } ],
                tie_breaker => 1,
                boost       => 2
            }
        },
        {   dis_max => {
                queries =>
                    [ { text => { k => 'v' } }, { text => { k => 'v' } } ],
                tie_breaker => 1,
                boost       => 2
            }
        },

    );
}

test_queries(
    "UNARY OPERATOR: -custom_score",
    "-custom_score: V",
    { -custom_score => 'V' },
    qr/HASHREF/,

    "-custom_score: {}",
    {   -custom_score => {
            query  => { k   => 'v' },
            script => 'script',
            lang   => 'lang',
            params => { foo => 'bar' }
        }
    },
    {   custom_score => {
            query  => { text => { k => 'v' } },
            script => 'script',
            lang   => 'lang',
            params => { foo => 'bar' }
        }
    },

    "-not_custom_score: {}",
    {   -not_custom_score => {
            query  => { k   => 'v' },
            script => 'script',
            lang   => 'lang',
            params => { foo => 'bar' }
        }
    },
    {   bool => {
            must_not => [ {
                    custom_score => {
                        query  => { text => { k => 'v' } },
                        script => 'script',
                        lang   => 'lang',
                        params => { foo => 'bar' }
                    }
                }
            ]
        }
    },
);

test_queries(
    'UNARY OPERATOR: has_child, not_has_child',

    'HAS_CHILD: V',
    { -has_child => 'V' },
    qr/HASHREF/,

    'HAS_CHILD: %V',
    {   -has_child =>
            { query => { foo => 'bar' }, type => 'foo', _scope => 'scope' }
    },
    {   has_child => {
            query  => { text => { foo => 'bar' } },
            _scope => 'scope',
            type   => 'foo'
        }
    },

    'NOT_HAS_CHILD: %V',
    {   -not_has_child =>
            { query => { foo => 'bar' }, type => 'foo', _scope => 'scope' }
    },
    {   bool => {
            must_not => [ {
                    has_child => {
                        query  => { text => { foo => 'bar' } },
                        _scope => 'scope',
                        type   => 'foo'
                    }
                }
            ]
        }
    },

);

test_queries(
    'UNARY OPERATOR: -top_children, -not_top_children',

    '-top_children: V',
    { -top_children => 'V' },
    qr/HASHREF/,

    '-top_children: %V',
    {   -top_children => {
            query              => { foo => 'bar' },
            type               => 'foo',
            _scope             => 'scope',
            score              => 'max',
            factor             => 10,
            incremental_factor => 2
        }
    },
    {   top_children => {
            query  => { text => { foo => 'bar' } },
            _scope => 'scope',
            type   => 'foo',
            ,
            score              => 'max',
            factor             => 10,
            incremental_factor => 2
        }
    },

    '-not_top_children: %V',
    {   -not_top_children => {
            query              => { foo => 'bar' },
            type               => 'foo',
            _scope             => 'scope',
            score              => 'max',
            factor             => 10,
            incremental_factor => 2
        }
    },
    {   bool => {
            must_not => [ {
                    top_children => {
                        query  => { text => { foo => 'bar' } },
                        _scope => 'scope',
                        type   => 'foo',
                        ,
                        score              => 'max',
                        factor             => 10,
                        incremental_factor => 2
                    }
                },
            ]
        }
    },

);

test_queries(
    'UNARY OPERATOR: -filter -not_filter',
    'FILTER: {}',
    { -filter => { k => 'v' } },
    { constant_score => { filter => { term => { k => 'v' } } } },

    'NOT_FILTER: {}',
    { -not_filter => { k => 'v' } },
    { constant_score => { filter => { not => { term => { k => 'v' } } } } },

    'QUERY/FILTER',
    { k => 'v', -filter => { k => 'v' } },
    {   filtered => {
            query  => { text => { k => 'v' } },
            filter => { term => { k => 'v' } }
        }
    },
);

done_testing();

#===================================
sub test_queries {
#===================================
    note "\n" . shift();
    while (@_) {
        my $name = shift;
        my $in   = shift;
        my $out  = shift;
        if ( ref $out eq 'Regexp' ) {
            throws_ok { $a->query($in) } $out, $name;
        }
        else {
            eval {
                eq_or_diff scalar $a->query($in), { query => $out }, $name;
                1;
            }
                or die "*** FAILED TEST $name:***\n$@";
        }
    }
}

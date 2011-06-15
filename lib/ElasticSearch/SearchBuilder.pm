package ElasticSearch::SearchBuilder;

use Carp;
use strict;
use warnings;
use Scalar::Util ();

our $VERSION = '0.01';

my %SPECIAL_OPS = (
    query => {
        '='      => [ 'terms',    0 ],
        '=='     => [ 'terms',    0 ],
        '!='     => [ 'terms',    1 ],
        '<>'     => [ 'terms',    1 ],
        'in'     => [ 'terms',    0 ],
        'not_in' => [ 'terms',    1 ],
        '>'      => [ 'range',    0 ],
        '>='     => [ 'range',    0 ],
        '<'      => [ 'range',    0 ],
        '<='     => [ 'range',    0 ],
        'gt'     => [ 'range',    0 ],
        'lt'     => [ 'range',    0 ],
        'gte'    => [ 'range',    0 ],
        'lte'    => [ 'range',    0 ],
        '^'      => [ 'prefix',   0 ],
        '*'      => [ 'wildcard', 0 ],
    },
    filter => {
        '='           => [ 'terms',  0 ],
        '=='          => [ 'terms',  0 ],
        '!='          => [ 'terms',  1 ],
        '<>'          => [ 'terms',  1 ],
        'in'          => [ 'terms',  0 ],
        'not_in'      => [ 'terms',  1 ],
        '>'           => [ 'range',  0 ],
        '>='          => [ 'range',  0 ],
        '<'           => [ 'range',  0 ],
        '<='          => [ 'range',  0 ],
        'gt'          => [ 'range',  0 ],
        'lt'          => [ 'range',  0 ],
        'gte'         => [ 'range',  0 ],
        'lte'         => [ 'range',  0 ],
        '^'           => [ 'prefix', 0 ],
        'exists'      => [ 'exists', 0 ],
        'missing'     => [ 'exists', 0 ],
        'not_exists'  => [ 'exists', 1 ],
        'not_missing' => [ 'exists', 1 ],
    }
);

my %RANGE_MAP = (
    '>'  => 'gt',
    '<'  => 'lt',
    '>=' => 'gte',
    '<=' => 'lte'
);

#===================================
sub new {
#===================================
    my $self  = shift;
    my $class = ref($self) || $self;
    my %opt   = ( ref $_[0] eq 'HASH' ) ? %{ $_[0] } : @_;

    return bless \%opt, $class;
}

#===================================
sub query  { shift->_top_recurse( 'query',  @_ ) }
sub filter { shift->_top_recurse( 'filter', @_ ) }
#===================================

#======================================================================
# top-level
#======================================================================

#===================================
sub _top_ARRAYREF {
#===================================
    my ( $self, $type, $params, $logic ) = @_;
    $logic ||= 'or';

    my @args = @$params;
    my @clauses;

    while ( my $el = shift @args ) {
        my $clause = $self->_SWITCH_refkind(
            'ARRAYREFs',
            $el,
            {   ARRAYREF => sub {
                    $self->_recurse( $type, $el ) if @$el;
                },
                HASHREF => sub {
                    $self->_recurse( $type, $el, 'and' ) if %$el;
                },
                SCALAR => sub {
                    $self->_recurse( $type, { $el => shift(@args) } );
                },
                UNDEF => sub { croak "UNDEF in arrayref not supported" },
            }
        );
        push @clauses, $clause if $clause;
    }
    return $self->_join_clauses( $type, $logic, \@clauses );
}

#===================================
sub _top_HASHREF {
#===================================
    my ( $self, $type, $params ) = @_;

    my ( @clauses, $filter );

    for my $k ( sort keys %$params ) {
        my $v = $params->{$k};

        my $clause;

        # ($k => $v) is either a special unary op or a regular hashpair
        if ( $k =~ /^-./ ) {
            my $op = substr $k, 1;
            if ( $op eq 'filter' and $type eq 'query' ) {
                $filter = $self->_recurse( 'filter', $v );
                next;
            }

            my $not = $op =~ s/^not_//;
            croak "Invalid op 'not_$op"
                if $not and $op eq 'cache' || $op eq 'nocache';

            my $handler = $self->can("_${type}_unary_$op")
                or croak "Unknown $type op '$op'";

            $clause = $handler->( $self, $v );
            $clause = $self->_negate_clause( $type, $clause )
                if $not;
        }
        else {
            my $method = $self->_METHOD_FOR_refkind( "_hashpair", $v );
            $clause = $self->$method( $type, $k, $v );
        }
        push @clauses, $clause if $clause;
    }

    my $clause = $self->_join_clauses( $type, 'and', \@clauses );

    return $clause unless $filter;
    return $clause
        ? { filtered => { query => $clause, filter => $filter } }
        : { constant_score => { filter => $filter } };
}

#===================================
sub _top_SCALARREF {
#===================================
    my ( $self, $type, $params ) = @_;
    return ($$params);
}

#===================================
sub _top_SCALAR {
#===================================
    my ( $self, $type, $params ) = @_;
    return $type eq 'query'
        ? { text => { _all => $params } }
        : { term => { _all => $params } };
}

#===================================
sub _top_UNDEF {
#===================================
    return ();
}

#======================================================================
# HASH PAIRS
#======================================================================

#===================================
sub _hashpair_ARRAYREF {
#===================================
    my ( $self, $type, $k, $v ) = @_;

    my @v = @$v ? @$v : [undef];

    # put apart first element if it is an operator (-and, -or)
    my $op
        = $v[0] && ( $v[0] eq '-and' || $v[0] eq '-or' )
        ? shift @v
        : '';

    my $scalars     = 0;
    my @distributed = map {
        defined and !ref and $scalars++;
        +{ $k => $_ }
    } @v;

    my $logic = $op ? substr( $op, 1 ) : '';

    # if all values are defined scalars then try to use
    # a terms query/filter

    if ( $scalars == @distributed ) {
        if ( $logic eq 'and' ) {
            if ( $type eq 'query' ) {
                return $self->_query_field_terms(
                    $k, 'terms',
                    {   value         => \@v,
                        minimum_match => $scalars
                    }
                );
            }
        }
        else {
            return $type eq 'query'
                ? $self->_query_field_terms( $k, 'terms', \@v )
                : $self->_filter_field_terms( $k, 'terms', \@v );
        }
    }

    unshift @distributed, $op
        if $op;

    return $self->_recurse( $type, \@distributed, $logic );
}

#===================================
sub _hashpair_HASHREF {
#===================================
    my ( $self, $type, $k, $v, $logic ) = @_;
    $logic ||= 'and';

    my @clauses;

    for my $orig_op ( sort keys %$v ) {
        my $clause;

        my $val = $v->{$orig_op};
        my $op  = $orig_op;
        $op =~ s/^-//;

        if ( my $hash_op = $SPECIAL_OPS{$type}{$op} ) {
            my ( $handler, $not ) = @$hash_op;
            $handler = "_${type}_field_$handler";
            $clause  = $self->$handler( $k, $op, $val );
            $clause  = $self->_negate_clause( $type, $clause )
                if $not;
        }
        else {
            my $not = ( $op =~ s/^not_// );
            my $handler = "_${type}_field_$op";
            croak "Unknown $type operator '$op'"
                unless $self->can($handler);

            $clause = $self->$handler( $k, $op, $val );
            $clause = $self->_negate_clause( $type, $clause )
                if $not;
        }
        push @clauses, $clause;
    }

    return $self->_join_clauses( $type, $logic, \@clauses );
}

#===================================
sub _hashpair_SCALARREF {
#===================================
    my ( $self, $type, $k, $v ) = @_;
    return { $k => $$v };
}

#===================================
sub _hashpair_SCALAR {
#===================================
    my ( $self, $type, $k, $v ) = @_;
    return { term => { $k => $v } };
}

#===================================
sub _hashpair_UNDEF {
#===================================
    my ( $self, $type, $k, $v ) = @_;
    return { missing => { field => $k } }
        if $type eq 'filter';
    croak "$k => UNDEF not a supported query op";
}

#======================================================================
# CLAUSE UTILS
#======================================================================

#===================================
sub _negate_clause {
#===================================
    my ( $self, $type, $clause ) = @_;
    return $type eq 'filter'
        ? { not => $clause }
        : $self->_merge_bool_queries( 'must_not', [$clause] );
}

#===================================
sub _join_clauses {
#===================================
    my ( $self, $type, $logic, $clauses ) = @_;

    return if @$clauses == 0;
    return $clauses->[0] if @$clauses == 1;

    if ( $logic eq 'and' ) {
        $clauses = $self->_merge_range_clauses($clauses);
        return $clauses->[0] if @$clauses == 1;
    }
    if ( $type eq 'query' ) {
        my $op = $logic eq 'and' ? 'must' : 'should';
        return $self->_merge_bool_queries( $op, $clauses );

    }
    return { $logic => $clauses };
}

#===================================
sub _merge_bool_queries {
#===================================
    my $self    = shift;
    my $op      = shift;
    my $queries = shift;
    my %bool;
    for my $query (@$queries) {
        my ( $type, $clauses ) = %$query;
        if ( $type eq 'bool' ) {
            my @keys = keys %$clauses;
            if ( $op eq 'must_not' ) {
                if ( @keys == 1 and $keys[0] eq 'must_not' ) {
                    push @{ $bool{must} }, @{ $clauses->{must_not} };
                    next;
                }
                if ( !$clauses->{should} and @$queries == 1 ) {
                    my ( $must, $not )
                        = delete @{$clauses}{ 'must', 'must_not' };
                    $clauses->{must_not} = $must if $must;
                    $clauses->{must}     = $not  if $not;
                }
            }

            if (   $op eq 'must'
                or $op eq 'should' and @keys == 1 and $clauses->{$op}
                or $op eq 'must_not' and !$clauses->{should} )
            {
                push @{ $bool{$_} }, @{ $clauses->{$_} } for keys %$clauses;
                next;
            }
        }
        push @{ $bool{$op} }, $query;
    }
    if ( keys %bool == 1 ) {
        my ( $k, $v ) = %bool;
        return $v->[0]
            if $k ne 'must_not' and @$v == 1;
    }

    return { bool => \%bool };
}

my %Range_Clauses = (
    range         => 1,
    numeric_range => 1,
);

#===================================
sub _merge_range_clauses {
#===================================
    my $self    = shift;
    my $clauses = shift;
    my ( @new, %merge );

    for (@$clauses) {
        my ($type) = keys %$_;

        if ( $Range_Clauses{$type} and not exists $_->{$type}{_cache} ) {
            my ( $field, $constraint ) = %{ $_->{$type} };

            for my $op ( keys %$constraint ) {
                if ( defined $merge{$type}{$field}{$op} ) {
                    croak "Duplicate '$type:$op' exists "
                        . "for field '$field', with values: "
                        . $merge{$type}{$field}{$op} . ' and '
                        . $constraint->{$op};
                }

                $merge{$type}{$field}{$op} = $constraint->{$op};
            }
        }
        else { push @new, $_ }
    }

    for my $type ( keys %merge ) {
        for my $field ( keys %{ $merge{$type} } ) {
            push @new, { $type => { $field => $merge{$type}{$field} } };
        }
    }
    return \@new;
}

#======================================================================
# UNARY OPS
#======================================================================
# Shared query/filter unary ops
#======================================================================

#===================================
sub _query_unary_or  { shift->_unary_and( 'query', shift, 'or' ) }
sub _query_unary_and { shift->_unary_and( 'query', shift, 'and' ) }
sub _query_unary_not { shift->_unary_not( 'query', shift, ) }
sub _query_unary_ids { shift->_unary_ids( 'query', shift ) }
sub _query_unary_has_child { shift->_unary_child( 'query', shift ) }
#===================================

#===================================
sub _filter_unary_or  { shift->_unary_and( 'filter', shift, 'or' ) }
sub _filter_unary_and { shift->_unary_and( 'filter', shift, 'and' ) }
sub _filter_unary_not { shift->_unary_not( 'filter', shift, ) }
sub _filter_unary_ids { shift->_unary_ids( 'filter', shift ) }
sub _filter_unary_has_child { shift->_unary_child( 'filter', shift ) }
#===================================

#===================================
sub _unary_and {
#===================================
    my ( $self, $type, $v, $op ) = @_;
    $self->_SWITCH_refkind(
        "Unary -$op",
        $v,
        {   ARRAYREF => sub { return $self->_top_ARRAYREF( $type, $v, $op ) },
            HASHREF => sub {
                return $op eq 'or'
                    ? $self->_top_ARRAYREF( $type,
                    [ map { $_ => $v->{$_} } ( sort keys %$v ) ], $op )
                    : $self->_top_HASHREF( $type, $v );
            },

            SCALAR => sub {
                croak "$type -$op => '$v' makes little sense, "
                    . "use filter -exists => '$v' instead";
            },
            UNDEF => sub { croak "$type -$op => undef not supported" },
        }
    );
}

#===================================
sub _unary_not {
#===================================
    my ( $self, $type, $v ) = @_;
    my $clause = $self->_SWITCH_refkind(
        "Unary -not",
        $v,
        {   ARRAYREF => sub { $self->_top_ARRAYREF( $type, $v ) },
            HASHREF  => sub { $self->_top_HASHREF( $type,  $v ) },
            SCALAR   => sub {
                croak "$type -not => '$v' makes little sense, "
                    . "use filter -missing => '$v' instead";
            },
            UNDEF => sub { croak "$type -not => undef not supported" },
        }
    ) or return;

    return $self->_negate_clause( $type, $clause );
}

#===================================
sub _unary_ids {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary -ids",
        $v,
        {   SCALAR   => { return { ids => { values => [$v] } } },
            ARRAYREF => sub {
                return unless @$v;
                return { ids => { values => $v } };
            },
            HASHREF => sub {
                my $p
                    = $self->_hash_params( 'ids', $v, ['values'], ['type'] );
                return { ids => $p };
            },
        }
    );
}

#===================================
sub _query_unary_top_children {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -top_children",
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params(
                    'top_children', $v,
                    [ 'query', 'type' ],
                    [qw(_scope score factor incremental_factor)]
                );
                $p->{query} = $self->_recurse( 'query', $p->{query} );
                return { top_children => $p };
            },
        }
    );
}

#===================================
sub _unary_child {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary $type -has_child",
        $v,
        {   HASHREF => sub {
                my $p
                    = $self->_hash_params( 'has_child', $v,
                    [ 'query', 'type' ],
                    ['_scope'] );
                $p->{query} = $self->_recurse( 'query', $p->{query} );
                return { has_child => $p };
            },
        }
    );
}

#======================================================================
# Query only unary ops
#======================================================================

#===================================
sub _query_unary_query_string {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -query_string",
        $v,
        {   SCALAR  => sub { return { query_string => { query => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'query_string',
                    $v,
                    ['query'],
                    [   qw(allow_leading_wildcard analyzer analyze_wildcard
                            auto_generate_phrase_queries boost
                            default_operator enable_position_increments
                            fields fuzzy_min_sim fuzzy_prefix_length
                            lowercase_expanded_terms phrase_slop
                            tie_breaker use_dis_max)
                    ]
                );
                return { query_string => $p };
            },
        }
    );
}

#===================================
sub _query_unary_flt {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -flt",
        $v,
        {   SCALAR  => sub { return { flt => { like_text => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'flt', $v,
                    ['like_text'],
                    [   qw(boost fields ignore_tf max_query_terms
                            min_similarity prefix_length )
                    ]
                );
                return { flt => $p };
            },
        }
    );
}

#===================================
sub _query_unary_mlt {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -mlt",
        $v,
        {   SCALAR  => sub { return { mlt => { like_text => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'mlt', $v,
                    ['like_text'],
                    [   qw(boost boost_terms fields
                            max_doc_freq max_query_terms max_word_len
                            min_doc_freq min_term_freq min_word_len
                            percent_terms_to_match stop_words )
                    ]
                );
                return { mlt => $p };
            },
        }
    );
}

#===================================
sub _query_unary_custom_score {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -custom_score",
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params(
                    'custom_score', $v,
                    [ 'query',  'script' ],
                    [ 'params', 'lang' ]
                );
                return { custom_score => $p };
            },
        }
    );
}

#===================================
sub _query_unary_dismax { shift->_query_unary_dis_max(@_) }
#===================================
#===================================
sub _query_unary_dis_max {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -dis_max",
        $v,
        {   ARRAYREF => sub {
                return $self->_query_unary_dis_max( { queries => $v } );
            },
            HASHREF => sub {
                my $p = $self->_hash_params( 'dis_max', $v, ['queries'],
                    [ 'boost', 'tie_breaker' ] );
                $p = $self->_multi_queries( $p, 'queries' );
                return { dis_max => $p };
            },
        }
    );
}

#===================================
sub _query_unary_bool {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -bool",
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params(
                    'bool', $v,
                    [],
                    [   qw(must should must_not boost
                            minimum_number_should_match disable_coord)
                    ]
                );
                $p = $self->_multi_queries( $p, 'must', 'should',
                    'should_not' );
                return { bool => $p };
            },
        }
    );
}

#===================================
sub _query_unary_boosting {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary query -boosting",
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params( 'dis_max', $v,
                    [ 'positive', 'negative', 'negative_boost' ] );
                $p->{$_} = $self->_recurse( 'query', $p->{$_} )
                    for 'positive', 'negative';
                return { boosting => $p };
            },
        }
    );
}

#======================================================================
# Filter only unary ops
#======================================================================

#===================================
sub _filter_unary_missing { shift->_filter_unary_exists( @_, 'missing' ) }
#===================================

#===================================
sub _filter_unary_exists {
#===================================
    my ( $self, $v, $op ) = @_;
    $op ||= 'exists';

    return $self->_SWITCH_refkind(
        "Unary filter -$op",
        $v,
        {   SCALAR => sub { return { $op => { field => $v } } }
        }
    );
}

#===================================
sub _filter_unary_type {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary filter -type",
        $v,
        {   SCALAR   => sub { return { type => { value => $v } } },
            ARRAYREF => sub { return { type => { value => $v } } },
        }
    );
}

#===================================
sub _filter_unary_script {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        "Unary filter -script",
        $v,
        {   SCALAR  => sub { return { script => { script => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params( 'script', $v, ['script'],
                    [ 'params', 'lang' ] );
                return { script => $p };
            },
        }
    );
}

#===================================
sub _filter_unary_query { shift->query(@_) }
#===================================

#===================================
sub _filter_unary_nocache { shift->_filter_unary_cache( @_, 'nocache' ) }
#===================================
#===================================
sub _filter_unary_cache {
#===================================
    my ( $self, $v, $op ) = @_;
    $op ||= 'cache';
    my $filter = $self->_SWITCH_refkind(
        "Unary filter -$op",
        $v,
        {   ARRAYREF => sub { $self->_recurse( 'filter', $v ) },
            HASHREF  => sub { $self->_recurse( 'filter', $v ) },
        }
    );

    return unless $filter;

    my ($type) = keys %$filter;
    if ( $type eq 'query' ) {
        $filter = { fquery => $filter };
        $type = 'fquery';
    }
    $filter->{$type}{_cache} = $op eq 'cache' ? 1 : 0;
    return $filter;
}

#======================================================================
# FIELD OPS
#======================================================================
# Query field ops
#======================================================================

#===================================
sub _query_field_prefix {
#===================================
    shift->_query_field_generic( @_, 'prefix', ['value'], ['boost'] );
}

#===================================
sub _query_field_wildcard {
#===================================
    shift->_query_field_generic( @_, 'wildcard', ['value'], ['boost'] );
}

#===================================
sub _query_field_fuzzy {
#===================================
    shift->_query_field_generic( @_, 'fuzzy', ['value'],
        [qw(boost min_similarity max_expansions prefix_length)] );
}

#===================================
sub _query_field_text {
#===================================
    shift->_query_field_generic( @_, 'text', ['query'],
        [qw(boost operator analyzer fuzziness max_expansions prefix_length)]
    );
}

#===================================
sub _query_field_phrase {
#===================================
    shift->_query_field_generic( @_, 'phrase', ['query'],
        [qw(boost slop analyzer)] );
}

#===================================
sub _query_field_phrase_prefix {
#===================================
    shift->_query_field_generic( @_, 'phrase_prefix', ['query'],
        [qw(boost analyzer slop max_expansions)] );
}

#===================================
sub _query_field_field {
#===================================
    shift->_query_field_generic(
        @_, 'field',
        ['query'],
        [   qw(default_operator analyzer allow_leading_wildcard
                lowercase_expanded_terms enable_position_increments
                fuzzy_prefix_length fuzzy_min_sim phrase_slop boost
                analyze_wildcard auto_generate_phrase_queries)
        ]
    );
}

#===================================
sub _query_field_generic {
#===================================
    my ( $self, $k, $orig_op, $val, $op, $req, $opt ) = @_;

    return $self->_SWITCH_refkind(
        "Query field operator -$orig_op",
        $val,
        {   SCALAR   => sub { return { $op => { $k => $val } } },
            ARRAYREF => sub {
                my $method = "_query_field_${op}";
                my @queries
                    = map { $self->$method( $k, $orig_op, $_ ) } @$val;
                return $self->_join_clauses( 'query', 'or', \@queries ),;
            },
            HASHREF => sub {
                my $p = $self->_hash_params( $orig_op, $val, $req, $opt );
                return { $op => { $k => $p } };
            },
        }
    );
}

#===================================
sub _query_field_terms {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Query field operator -$op",
        $val,
        {   UNDEF   => sub { $self->_hashpair_UNDEF( 'query',  $k, $val ) },
            SCALAR  => sub { $self->_hashpair_SCALAR( 'query', $k, $val ) },
            HASHREF => sub {
                my $v = delete $val->{value};
                $v = $v->[0] if ref $v eq 'ARRAY' and @$v < 2;
                croak "Missing 'value' param in 'terms' query"
                    unless defined $v;

                if ( ref $v eq 'ARRAY' ) {
                    my $p = $self->_hash_params( $op, $val, [],
                        [ 'boost', 'minimum_match' ] );
                    $p->{$k} = $v;
                    return { terms => $p };
                }
                delete $val->{minimum_match};
                my $p = $self->_hash_params( $op, $val, [], ['boost'] );
                $p->{value} = $v;
                return { term => { $k => $p } };
            },
            ARRAYREF => sub {
                my @scalars = grep { defined && !ref } @$val;
                if ( @scalars == @$val && @scalars > 1 ) {
                    return { terms => { $k => $val } };
                }
                my @queries;
                for (@$val) {
                    my $query = $self->_query_field_terms( $k, $op, $_ )
                        or next;
                    push @queries, $query;
                }
                return $self->_join_clauses( 'query', 'or', \@queries );
            },

        },
    );
}

#===================================
sub _query_field_mlt {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Query field operator -$op",
        $val,
        {   SCALAR => sub {
                return { mlt_field => { $k => { like_text => $val } } };
            },
            ARRAYREF => sub {
                my @queries
                    = map { $self->_query_field_mlt( $k, $op, $_ ) } @$val;
                return $self->_join_clauses( 'query', 'or', \@queries ),;
            },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    $op, $val,
                    ['like_text'],
                    [   qw( boost boost_terms max_doc_freq
                            max_query_terms max_word_len min_doc_freq
                            min_term_freq min_word_len
                            percent_terms_to_match stop_words )
                    ]
                );
                return { mlt_field => { $k => $p } };
            },
        }
    );
}

#===================================
sub _query_field_flt {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Query field operator -$op",
        $val,
        {   SCALAR => sub {
                return { flt_field => { $k => { like_text => $val } } };
            },
            ARRAYREF => sub {
                my @queries
                    = map { $self->_query_field_flt( $k, $op, $_ ) } @$val;
                return $self->_join_clauses( 'query', 'or', \@queries ),;
            },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    $op, $val,
                    ['like_text'],
                    [   qw( boost ignore_tf max_query_terms
                            min_similarity prefix_length)
                    ]
                );
                return { flt_field => { $k => $p } };
            },
        }
    );
}

#===================================
sub _query_field_range {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    my $es_op = $RANGE_MAP{$op} || $op;

    return $self->_SWITCH_refkind(
        "Query field operator -$op",
        $val,
        {   HASHREF => sub {
                my $p = $self->_hash_params(
                    'range', $val,
                    [],
                    [   qw(from to include_lower include_upper
                            gt gte lt lte boost)
                    ]
                );
                return { range => { $k => $p } };
            },
            SCALAR => sub {
                croak "range op does not accept a scalar. Instead, use "
                    . "a comparison operator, eg: gt, lt"
                    if $es_op eq 'range';
                return { 'range' => { $k => { $es_op => $val } } };
            },
        }
    );
}

#======================================================================
# Filter field ops
#======================================================================

#===================================
sub _filter_field_terms {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Filter field operator -$op",
        $val,
        {   UNDEF  => sub { $self->_hashpair_UNDEF( 'filter',  $k, $val ) },
            SCALAR => sub { $self->_hashpair_SCALAR( 'filter', $k, $val ) },
            ARRAYREF => sub {
                my @scalars = grep { defined && !ref } @$val;
                if ( @scalars == @$val && @scalars > 1 ) {
                    return { terms => { $k => $val } };
                }
                my @filters;
                for (@$val) {
                    my $filter = $self->_filter_field_terms( $k, $op, $_ )
                        or next;
                    push @filters, $filter;
                }
                return $self->_join_clauses( 'filter', 'or', \@filters );
            },
        }
    );
}

#===================================
sub _filter_field_range {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    my ( $type, $es_op );
    if ( $es_op = $RANGE_MAP{$op} ) {
        $type = 'numeric_range';
    }
    else {
        $es_op = $op;
        $type  = 'range';
    }

    return $self->_SWITCH_refkind(
        "Filter field operator -$op",
        $val,
        {   SCALAR => sub {
                return { $type => { $k => { $es_op => $val } } };
            },
        }
    );
}

#===================================
sub _filter_field_prefix {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Filter field operator -$op",
        $val,
        {   SCALAR   => sub { return { prefix => { $k => $val } } },
            ARRAYREF => sub {
                my @filters
                    = map { $self->_filter_field_prefix( $k, $op, $_ ) }
                    @$val;
                return $self->_join_clauses( 'filter', 'or', \@filters ),;
            },
        }
    );
}

#===================================
sub _filter_field_exists {
#===================================
    my ( $self, $k, $op, $val ) = @_;
    $val ||= 0;
    $val = !$val if $op =~ s/^not_//;

    return $self->_SWITCH_refkind(
        "Filter field operator -$op",
        $val,
        {   SCALAR => sub {
                if ( $op eq 'missing' ) { $val = !$val }
                return { ( $val ? 'exists' : 'missing' ) => { field => $k } };
            },
        }
    );
}

#===================================
sub _filter_field_geo_bounding_box {
#===================================
    my $self = shift;
    my $k    = shift;
    my $p    = $self->_hash_params( @_, [qw(top_left bottom_right)] );
    return { geo_bounding_box => { $k => $p } };
}

#===================================
sub _filter_field_geo_distance {
#===================================
    my $self = shift;
    my $k    = shift;
    my $p    = $self->_hash_params( @_, [qw(distance location)] );
    return {
        geo_distance => {
            distance => $p->{distance},
            $k       => $p->{location}
        }
    };
}

#===================================
sub _filter_field_geo_distance_range {
#===================================
    my $self = shift;
    my $k    = shift;
    my $p    = $self->_hash_params( @_, ['location'],
        [qw(from to gt lt gte lte include_upper include_lower)] );
    $p->{$k} = delete $p->{location};
    return { geo_distance_range => $p };
}

#===================================
sub _filter_field_geo_polygon {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        "Filter field operator -$op",
        $val,
        {   ARRAYREF => sub {
                return { geo_polygon => { $k => { points => $val } } };
            },
        }
    );
}

#======================================================================
# UTILITIES
#======================================================================

#===================================
sub _top_recurse {
#===================================
    my $self   = shift;
    my $type   = shift;
    my $params = shift;
    croak "Too many params passed to ${type}()"
        if @_;
    my $clause = $self->_recurse( $type, $params );
    return $clause ? { $type => $clause } : ();
}

#===================================
sub _recurse {
#===================================
    my ( $self, $type, $params, $logic ) = @_;

    my $method = $self->_METHOD_FOR_refkind( "_top", $params );
    return $self->$method( $type, $params, $logic );
}

#===================================
sub _refkind {
#===================================
    my ( $self, $data ) = @_;

    return 'UNDEF' unless defined $data;

    # blessed objects are treated like scalars
    my $ref = ( Scalar::Util::blessed $data) ? '' : ref $data;

    return 'SCALAR' unless $ref;

    my $n_steps = 1;
    while ( $ref eq 'REF' ) {
        $data = $$data;
        $ref = ( Scalar::Util::blessed $data) ? '' : ref $data;
        $n_steps++ if $ref;
    }

    return ( $ref || 'SCALAR' ) . ( 'REF' x $n_steps );
}

#===================================
sub _try_refkind {
#===================================
    my ( $self, $data ) = @_;
    my @try = ( $self->_refkind($data) );
    push @try, 'SCALAR_or_UNDEF'
        if $try[0] eq 'SCALAR' || $try[0] eq 'UNDEF';
    push @try, 'FALLBACK';
    return \@try;
}

#===================================
sub _METHOD_FOR_refkind {
#===================================
    my ( $self, $meth_prefix, $data ) = @_;

    my $method;
    for ( @{ $self->_try_refkind($data) } ) {
        $method = $self->can( $meth_prefix . "_" . $_ )
            and last;
    }

    return $method
        || croak "cannot dispatch on '$meth_prefix' for "
        . $self->_refkind($data);
}

#===================================
sub _SWITCH_refkind {
#===================================
    my ( $self, $op, $data, $dispatch_table ) = @_;

    my $coderef;
    for ( @{ $self->_try_refkind($data) } ) {
        $coderef = $dispatch_table->{$_}
            and last;
    }

    unless ($coderef) {
        croak "$op only accepts parameters of type: "
            . join( ', ', sort keys %$dispatch_table ) . "\n";
    }

    return $coderef->();
}

#===================================
sub _hash_params {
#===================================
    my ( $self, $op, $val, $req, $opt ) = @_;

    croak "Op '$op' only accepts a hashref"
        unless ref $val eq 'HASH';

    my %params;
    for (@$req) {
        $params{$_} = delete $val->{$_}
            or croak "'$op' missing required param '$_'";
    }
    if ($opt) {
        for (@$opt) {
            $params{$_} = delete $val->{$_} || next;
        }
    }

    croak "Unknown param(s) for '$op': " . join( ', ', keys %$val )
        if %$val;

    return \%params;
}

#===================================
sub _multi_queries {
#===================================
    my $self   = shift;
    my $params = shift;
    for my $key (@_) {
        my $v = $params->{$key} or next;
        my @q = ref $v eq 'ARRAY' ? @$v : $v;
        $params->{$key} = [ map { $self->_recurse( 'query', $_ ) } @q ];
    }
    return $params;
}

1;

=head1 NAME

ElasticSearch::SearchBuilder - A Perlish compact query language for ElasticSearch

=head1 VERSION

Version 0.01

=cut

=head1 DESCRIPTION

The Query DSL for ElasticSearch (see L<http://www.elasticsearch.org/guide/reference/query-dsl>),
which is used to write queries and filters,
is simple but verbose, which can make it difficult to write and understand
large queries.

L<ElasticSearch::SearchBuilder> is an L<SQL::Abstract>-like query language
which exposes the full power of the query DSL, but in a more compact,
Perlish way.

B<THIS MODULE IS NOT READY TO USE - IT IS COMPLETELY UNTESTED ALPHA CODE>

=cut

=head1 SYNOPSIS

    my $sb = ElasticSearch::SearchBuilder->new();
    my $query = $sb->query({
        body    => {text => 'interesting keywords'},
        -filter => {
            status  => 'active',
            tags    => ['perl','python','ruby'],
            created => {
                '>=' => '2010-01-01',
                '<'  => '2011-01-01'
            },
        }
    })

=cut

=head1 METHODS

=head2 new()

    my $sb = ElastiSearch::SearchBuilder->new()

Creates a new instance of the SearchBuilder - takes no parameters.

=head2 query()

    my $es_query = $sb->query($compact_query)

Returns a query in the ElasticSearch query DSL.

C<$compact_query> can be a scalar, a hash ref or an array ref.

    $sb->query('foo')
    # { "query" : { "text" : { "_all" : "foo" }}}

    $sb->query({ ... }) or $sb->query([ ... ])
    # { "query" : { ... }}

=head2 filter()

    my $es_filter = $sb->filter($compact_filter)

Returns a filter in the ElasticSearch query DSL.

C<$compact_filter> can be a scalar, a hash ref or an array ref.

    $sb->filter('foo')
    # { "filter" : { "term" : { "_all" : "foo" }}}

    $sb->filter({ ... }) or $sb->filter([ ... ])
    # { "filter" : { ... }}

=cut

=head1 INTRODUCTION

B<IMPORTANT>: If you are not familiar with ElasticSearch then you should
read L</"ELASTICSEARCH CONCEPTS"> before continuing.

This module was inspired by L<SQL::Abstract> but they are not compatible with
each other.

All constructs described below can be applied to both queries and filters,
unless stated otherwise. If using the method L</"-query"> then it starts off
in "query" mode, and if using the method L</"-filter"> then it starts off
in filter mode.  For example:

    $sb->query({

        # query mode
        foo     => 1,
        bar     => 2,

        -filter => {
            # filter mode
            foo     => 1,
            bar     => 2,

            -query  => {
                # query mode
                foo => 1
            }
        }
    })

The easiest way to explain how the syntax works is to give examples:

=head1 KEY-VALUE PAIRS

Key-value pairs are converted to term queries or term filters:

    # Field 'foo' contains term 'bar'
    { foo => 'bar' }

    # Field 'foo' contains 'bar' or 'baz'
    { foo => ['bar','baz']}

    # Field 'foo' contains terms 'bar' AND 'baz'
    { foo => ['-and','bar','baz']}

    ### FILTER ONLY ###

    # Field 'foo' is missing ie has no value
    { foo => undef }

=cut

=head2 AND/OR LOGIC

Arrays are OR'ed, hashes are AND'ed:

    # tags = 'perl' AND status = 'active:
    {
        tags   => 'perl',
        status => 'active'
    }

    # tags = 'perl' OR status = 'active:
    [
        tags   => 'perl',
        status => 'active'
    ]

    # tags = 'perl' or tags = 'python':
    { tags => [ 'perl','python' ]}
    { tags => { '=' => [ 'perl','python' ] }}

    # tags begins with prefix 'p' or 'r'
    { tags => { '^' => [ 'p','r' ] }}

The logic in an array can changed from OR to AND by making the first
element of the array ref C<-and>:

    # tags has term 'perl' AND 'python'

    { tags => ['-and','perl','python']}

    {
        tags => [
            -and => { '=' => 'perl'},
                    { '=' => 'python'}
        ]
    }

However, the first element in an array ref which is used as the value for
a field operator (see </"FIELD OPERATORS">) is not special:

    # WRONG
    { tags => { '=' => [ '-and','perl','python' ] }}

...otherwise you would never be able to search for the term C<-and>. So if
you might possibly have the terms C<-and> or C<-or> in your data, use:

    { foo => {'=' => [....] }}

instead of:

    { foo => [....]}

Also, see L</"NESTING AND COMBINING">.

=head2 FIELD OPERATORS

Most operators (eg C<=>, C<gt>, C<geo_distance> etc) are applied to a
particular field. These are known as C<Field Operators>. For example:

    # Field foo contains the term 'bar'
    { foo => 'bar' }
    { foo => {'=' => 'bar' }}

    # Field created is between Jan 1 and Dec 31 2010
    { created => {
        '>='  => '2010-01-01',
        '<'   => '2011-01-01'
    }}

    # Field foo contains terms which begin with prefix 'a' or 'b' or 'c'
    { foo => { '^' => ['a','b','c' ]}}

Some field operators are available as symbols (eg C<=>, C<*>, C<^>, C<gt>) and
others as words (eg C<geo_distance> or C<-geo_distance> - the dash is optional).

Multiple field operators can be applied to a single field.
Use C<{}> to imply C<this AND that>:

    # Field foo has any value from 100 to 200
    { foo => { gte => 100, lte => 200 }}

    # Field foo begins with 'p' but is not python
    { foo => {
        '^'  => 'p',
        '!=' => 'python'
    }}

Or C<[]> to imply C<this OR that>

    # foo is 5 or foo greater than 10
    { foo => [
        { '='  => 5  },
        { 'gt' => 10 }
    ]}

All word operators may be negated by adding C<not_> to the beginning, eg:

    # Field foo does NOT contain a term beginning with 'bar' or 'baz'
    { foo => { not_prefix => ['bar','baz'] }}


=head1 UNARY OPERATORS

There are other operators which don't fit this
C<< { field => { op => value}} >>model.

For instance:

=over

=item *

An operator might apply to multiple fields:

    # Search fields 'title' and 'content' for text 'brown cow'
    {
        -query_string => {
            query   => 'brown cow',
            fields  => ['title','content']
        }
    }

=item *

The field might BE the value:

    # Find documents where the field 'foo' is blank or undefined
    { -missing => 'foo' }

    # Find documents where the field 'foo' exists and has a value
    { -exists => 'foo' }

=item *

For combining other queries or filters:

    # Field foo has terms 'bar' and 'baz' but not 'balloo'
    {
        -and => [
            foo => 'bar',
            foo => 'baz',
            -not => { foo => 'balloo' }
        ]
    }

=item *

Other:

    # Script query
    { -script => "doc['num1'].value > 1" }

=back

These operators are called C<unary operators> and ALWAYS begin with a dash C<->
to distinguish them from field names.

Unary operators may also be prefixed with C<not_> to negate their meaning.

=cut

=head1 TERM QUERIES / FILTERS

=head2 = | == | in | != | <> | not_in

    # Field foo has the term 'bar':
    { foo => 'bar' }
    { foo => { '='  => 'bar' }}
    { foo => { '==' => 'bar' }}
    { foo => { 'in' => 'bar' }}

    # Field foo has the term 'bar' or 'baz'
    { foo => ['bar','baz'] }
    { foo => { '='  => ['bar','baz'] }}
    { foo => { '==' => ['bar','baz'] }}
    { foo => { 'in' => ['bar','baz'] }}

    # Field foo does not contain the term 'bar':
    { foo => { '!='     => 'bar' }}
    { foo => { 'not_in' => 'bar' }}

    # Field foo contains neither 'bar' nor 'baz'
    { foo => { '!='     => ['bar','baz'] }}
    { foo => { 'not_in' => ['bar','baz'] }}

*** For queries only ***

    # With query params
    { foo => {
        '=' => {
            value => 5,
            boost => 2
        }
    }}

    # With query params
    { foo => {
        '=' => {
            value         => [5,6],
            boost         => 2,
            minimum_match => 2,
        }
    }}

For term queries see:
L<http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html>
and
L<http://www.elasticsearch.org/guide/reference/query-dsl/terms-query.html>

For term filters see:
L<http://www.elasticsearch.org/guide/reference/query-dsl/term-filter.html>
and
L<http://www.elasticsearch.org/guide/reference/query-dsl/terms-filter.html>

=head2 ^ | prefix | not_prefix

    # Field foo contains a term which begins with 'bar'
    { foo => { '^'      => 'bar' }}
    { foo => { 'prefix' => 'bar' }}

    # Field foo contains a term which begins with 'bar' or 'baz'
    { foo => { '^'      => ['bar','baz'] }}
    { foo => { 'prefix' => ['bar','baz'] }}

    # Field foo contains a term which begins with neither 'bar' nor 'baz'
    { foo => { 'not_prefix' => ['bar','baz'] }}

*** For queries only ***

    # With query params
    { foo => {
        '^' => {
            value => 'bar',
            boost => 2
        }
    }}

For the prefix query see
L<http://www.elasticsearch.org/guide/reference/query-dsl/prefix-query.html>.

For the prefix filter see
L<http://www.elasticsearch.org/guide/reference/query-dsl/prefix-filter.html>

=head2 lt | gt | lte | gte | < | <= | >= | > | range | not_range

These operators imply a range query, which can be numeric or alphabetical.

    # Field foo contains terms between 'alpha' and 'beta'
    { foo => {
        'gte'   => 'alpha',
        'lte'   => 'beta'
    }}

    # Field foo contains numbers between 10 and 20
    { foo => {
        'gte'   => '10',
        'lte'   => '20'
    }}

*** For queries only ***

    # boost a range query
    { foo => {
        range => {
            gt      => 5,
            gte     => 5,
            lt      => 10,
            lte     => 10,
            boost         => 2.0
        }
    }}

B<Note>: for filter clauses, the C<gt>,C<gte>,C<lt> and C<lte> operators
imply a C<range> filter, while the C<< < >>, C<< <= >>, C<< > >> and C<< >= >>
operators imply a C<numeric_range> filter.

B<< This does not mean that you should use the C<numeric_range> version
for any field which contains numbers! >>

The C<numeric_range> query should be used for numbers/datetimes which
have many distinct values, eg C<ID> or C<last_modified>.  If you have a numeric
field with few distinct values, eg C<number_of_fingers> then it is better
to use a C<range> filter.

See L<http://www.elasticsearch.org/guide/reference/query-dsl/range-filter.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/numeric-range-filter.html>.

For queries, both sets of operators produce C<range> queries.

See L<http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html>


=head2 * | wildcard | not_wildcard

*** For queries only ***

A C<wildcard> query does a term query, but applies shell globbing to find
matching terms. In other words C<?> represents any single character,
while C<*> represents zero or more characters.

    # Field foo matches 'f?ob*'
    { foo => { '*'        => 'f?ob*' }}
    { foo => { 'wildcard' => 'f?ob*' }}

    # with a boost:
    { foo => {
        '*' => { value => 'f?ob*', boost => 2.0 }
    }}
    { foo => {
        'wildcard' => {
            value => 'f?ob*',
            boost => 2.0
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/wildcard-query.html>

=head2 fuzzy | not_fuzzy

*** For queries only ***

A C<fuzzy> query searches for terms that are similar to the the provided terms,
where similarity is based on the Levenshtein (edit distance) algorithm:

    # Field foo is similar to 'fonbaz'
    { foo => { fuzzy => 'fonbaz' }}

    # With other parameters:
    { foo => {
        fuzzy => {
            value           => 'fonbaz',
            boost           => 2.0,
            min_similarity  => 0.2,
            max_expansions  => 10
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/fuzzy-query.html>.


=cut

=head1 MISSING / EXISTS

You can use a C<missing> or C<exists> filter to select only docs where a
particular field exists and has a value, or is undefined or has no value:

*** For filters only ***

    # Field 'foo' has a value:
    { foo     => { exists  => 1 }}
    { foo     => { missing => 0 }}
    { -exists => 'foo'           }

    # Field 'foo' is undefined or has no value:
    { foo      => { missing => 1 }}
    { foo      => { exists  => 0 }}
    { -missing => 'foo'           }
    { foo      => undef           }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/missing-filter.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/exists-filter.html>

=head1 FULL TEXT SEARCH QUERIES

There are a range of full text search queries available, with varying
power, flexibility and complexity.

"Full text search" means that the text that you search on is analyzed
into terms before it is used by ElasticSearch.

See </"ELASTICSEARCH CONCEPTS"> for more.

*** For queries only ***

=head2 text | not_text

Perform a C<text> query on a field. C<text> queries are very flexible. For
analyzed text fields, they apply the correct analyzer and do a full text
search. For non-analyzed fields (numeric, date and non-analyzed strings) it
performs term queries:

    # Non-analyzed field 'status' has the term 'active'
    { status => {text => 'active' }}

    # Analyzed field 'content' includes the text "Brown Fox"
    { content => {text => 'Brown Fox' }}

    # Same as above but with extra parameters:
    { content => {
        text => {
            query          => 'Brown Fox',
            boost          => 2.0,
            operator       => 'and',
            analyzer       => 'default',
            fuzziness      => 0.5,
            max_expansions => 100,
            prefix_length  => 2,
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/text-query.html>

=head2 phrase | not_phrase

Performs a C<text_phrase> query.  For instance C<"Brown Fox"> will only match
if the phrase C<"brown fox"> is present.  Neither C<"fox brown"> nor
C<"Brown Wiley Fox"> will match.

    { content => { phrase=> "Brown Fox" }}

It accepts a C<slop> factor which will preserve the word order, but allow
the words themselves to have other words inbetween.  For instance, a C<slop>
of 3 will allow C<"Brown Wiley Fox"> to match, but C<"fox brown"> still
won't match.

    { content => {
        phrase => {
            query    => "Brown Fox",
            slop     => 3,
            analyzer => 'default',
            boost    => 3.0,
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/text-query.html>

=head2 phrase_prefix | not_phrase_prefix

Performs a C<text_phrase_prefix> query.  This is the sameas the L</"phrase">
query, but also does a C<prefix> query on the last term, which is useful
for auto-complete.

    { content => { phrase_prefix => "Brown Fo" }}

With extra options

    { content => {
        phrase_prefix => {
            query          => "Brown Fo",
            slop           => 3,
            analyzer       => 'default',
            boost          => 3.0,
            max_expansions => 100,
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/text-query.html>

=head2 field | not_field | -query_string | -not_query_string

A C<field> query or C<query_string> query does a full text query on the
provided text, and (unlike C<text>, C<phrase> or C<phrase_prefix> queries)
exposes all of the power of the Lucene query string syntax (see
L<http://lucene.apache.org/java/3_2_0/queryparsersyntax.html>).

C<field> queries are used to search on a single field, while C<-query_string>
queries are used to search on multiple fields.

    # search field foo for "this AND that"
    { foo => { field => 'this AND that' }}

    # With other parameters
    { foo => {
        field => {
            query                        => 'this AND that ',
            default_operator             => 'AND',
            analyzer                     => 'default',
            allow_leading_wildcard       => 0,
            lowercase_expanded_terms     => 1,
            enable_position_increments   => 1,
            fuzzy_prefix_length          => 2,
            fuzzy_min_sim                => 0.5,
            phrase_slop                  => 10,
            boost                        => 2,
            analyze_wildcard             => 1,
            auto_generate_phrase_queries => 0,
        }
    }}

    # multi-field searches:

    { -query_string => {
            query                        => 'this AND that ',
            fields                       => ['title','content'],
            default_operator             => 'AND',
            analyzer                     => 'default',
            allow_leading_wildcard       => 0,
            lowercase_expanded_terms     => 1,
            enable_position_increments   => 1,
            fuzzy_prefix_length          => 2,
            fuzzy_min_sim                => 0.5,
            phrase_slop                  => 10,
            boost                        => 2,
            analyze_wildcard             => 1,
            auto_generate_phrase_queries => 0,
            use_dis_max                  => 1,
            tie_breaker                  => 0.7
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/field-query.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html>
for more.

=head2 mlt | not_mlt

An C<mlt> or C<more_like_this> query finds documents that are "like" the
specified text, where "like" means that it contains some or all of the
specified terms.

    # Field foo is like "brown cow"
    { foo => { mlt => "brown cow" }}

    # With other paramters:
    { foo => {
        mlt => {
            like_text               => 'brown cow',
            percent_terms_to_match  => 0.3,
            min_term_freq           => 2,
            max_query_terms         => 25,
            stop_words              => ['the','and'],
            min_doc_freq            => 5,
            max_doc_freq            => 1000,
            min_word_len            => 0,
            max_word_len            => 20,
            boost_terms             => 2,
            boost                   => 2.0,
        }
    }}

    # multi fields
    { -mlt => {
        like_text               => 'brown cow',
        fields                  => ['title','content']
        percent_terms_to_match  => 0.3,
        min_term_freq           => 2,
        max_query_terms         => 25,
        stop_words              => ['the','and'],
        min_doc_freq            => 5,
        max_doc_freq            => 1000,
        min_word_len            => 0,
        max_word_len            => 20,
        boost_terms             => 2,
        boost                   => 2.0,
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/mlt-field-query.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/mlt-query.html>

=head2 flt | not_flt

An C<flt> or C<fuzzy_like_this> query fuzzifies all specified terms, then
picks the best C<max_query_terms> differentiating terms. It is a combination
of C<fuzzy> with C<more_like_this>.

    # Field foo is fuzzily similar to "brown cow"
    { foo => { flt => 'brown cow }}

    # With other parameters:
    { foo => {
        flt => {
            like_text       => 'brown cow',
            ignore_tf       => 0,
            max_query_terms => 10,
            min_similarity  => 0.5,
            prefix_length   => 3,
            boost           => 2.0,
        }
    }}

    # Multi-field
    flt => {
        like_text       => 'brown cow',
        fields          => ['title','content'],
        ignore_tf       => 0,
        max_query_terms => 10,
        min_similarity  => 0.5,
        prefix_length   => 3,
        boost           => 2.0,
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/flt-field-query.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/flt-query.html>

=head1 NESTING AND COMBINING

These constructs allow you to combine multiple queries and filters.

=head2 -filter

This allows you to combine a query with one or more filters:

*** For queries only ***

    # query field content for 'brown cow', and filter documents
    # where status is 'active' and tags contains the term 'perl'
    {
        content => { text => 'brown cow' },
        -filter => {
            status => 'active',
            tags   => 'perl'
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/filtered-query.html>

=head2 -query

This allows you to combine a filter with one or more queries:

*** For filters only ***

    # query field content for 'brown cow', and filter documents
    # where status is 'active', tags contains the term 'perl'
    # and a text query on field title contains 'important'
    {
        content => { text => 'brown cow' },
        -filter => {
            status => 'active',
            tags   => 'perl',
            -query => {
                title => { text => 'important' }
            }
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/query-filter.html>

=head2 -and | -or | -not

These operators allow you apply C<and>, C<or> and C<not> logic to nested
queries or filters.

    # Field foo has both terms 'bar' and 'baz'
    { -and => [
            foo => 'bar',
            foo => 'baz'
    ]}

    # Field
    { -or => [
        { name => { text => 'John Smith' }},
        {
            -missing => 'name',
            name     => { text => 'John Smith' }
        }
    ]}

The C<-and>, C<-or> and C<-not> constructs emit C<and>, C<or> and C<not> filters
for filters, and C<bool> queries for queries.

See L<http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html>,
C<http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html>,
C<http://www.elasticsearch.org/guide/reference/query-dsl/or-filter.html>
and C<http://www.elasticsearch.org/guide/reference/query-dsl/not-filter.html>.

=head2 -dis_max | -dismax

While a C<bool> query adds together the scores of the nested queries, a
C<dis_max> query uses the highest score of any matching queries.

*** For queries only ***

    # Run the two queries and use the best score
    { -dismax => [
        { foo => 'bar' },
        { foo => 'baz' }
    ] }

    # With other parameters
    { -dismax => {
        queries => [
            { foo => 'bar' },
            { foo => 'baz' }
        ],
        tie_breaker => 0.5,
        boost => 2.0
    ] }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/dis-max-query.html>

=head2 -bool

Normally, there should be no need to use a C<bool> query directly, as these
are autogenerated from eg C<-and>, C<-or> and C<-not> constructs. However,
if you need to pass any of the other parameters to a C<bool> query, then
you can do the following:

    {
       -bool => {
           must          => [{ foo => 'bar' }],
           must_not      => { status => 'inactive' },
           should        => [
                { tag    => 'perl'   },
                { tag    => 'python' },
                { tag    => 'ruby' },
           ],
           minimum_number_should_match => 2,
           disable_coord => 1,
           boost         => 2
       }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html>

=head2 -boosting

The C<boosting> query can be used to "demote" results that match a given query.
Unlike the C<must_not> clause of a C<bool> query, the query still matches,
but the results are "less relevant".

    { -boosting => {
        positive       => { title => { text => 'apple pear'     }},
        negative       => { title => { text => 'apple computer' }},
        negative_boost => 0.2
    }}

L<http://www.elasticsearch.org/guide/reference/query-dsl/boosting-query.html>

=head1 GEOLOCATION FILTERS

Geo-location filters work with fields that have the type C<geo_point>.
See L<http://www.elasticsearch.org/guide/reference/mapping/geo-point-type.html>)
for valid formats for the C<$location> field.

*** For filters only ***

=head2 geo_distance | not_geo_distance

Return docs with C<$distance> of C<$location>:

    # Field 'point' is within 100km of London
    { point => {
        geo_distance => {
            distance => '100km',
            location => {
                lat  => 51.50853,
                lon  => -0.12574
            }
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html>

=head2 geo_distance_range | not_geo_distance_range

This is like the range filter, and accepts the same parameters:

    # Field 'point' is 100-200km from London
    { point => {
        geo_distance_range => {
            gte      => '100km',
            lte      => '200km',
            location => {
                lat  => 51.50853,
                lon  => -0.12574
            }
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-range-filter.html>

=head2 geo_bounding_box | not_geo_bounding_box

This returns documents whose location lies within the specified
rectangle:

    { point => {
        geo_bounding_box => {
            top_left     => [40.73,-74.1],
            bottom_right => [40.71,-73.99],
        }
    }}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html>

=head2 geo_polygon | not_geo_polygon

This finds documents whose location lies within the specified polygon:

    { point => {
        geo_polygon => [[40,-70],[30,-80],[20,-90]]
    }}

L<http://www.elasticsearch.org/guide/reference/query-dsl/geo-polygon-filter.html>

=head1 SCRIPTING

ElasticSearch supports the use of scripts to customise query or filter
behaviour.  By default the query language is C<mvel> but javascript, groovy,
python and native java scripts are also supported.

See L<http://www.elasticsearch.org/guide/reference/modules/scripting.html> for
more on scripting.

=head2 -custom_score

The C<-custom_score> query allows you to customise the C<_score> or relevance
(and thus the order) of returned docs.

*** For queries only ***

    {
        -custom_score => {
            query  => { foo => 'bar' },
            lang    => 'mvel',
            script => "_score * doc['my_numeric_field'].value / pow(param1, param2)"
            params => {
                param1 => 2,
                param2 => 3.1
            },
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/custom-score-query.html>

=head2 -script

The C<-script> filter allows you to use a script as a filter. Return a true
value to indicate that the filter matches.

*** For filters only ***

    # Filter docs whose field 'foo' is greater than 5
    { -script => "doc['foo'].value > 5 " }

    # With other params
    {
        -script => {
            script => "doc['foo'].value > minimum ",
            params => { minimum => 5 },
            lang   => 'mvel'
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/script-filter.html>

=head1 TYPE/IDS

The C<_type> and C<_id> fields are not indexed by default, and thus aren't
available for normal queries or filters.

=head2 -ids

Returns docs with the matching C<_id> or C<_id>/C<_type> combination:

    # doc with ID 123
    { -ids => 123 }

    # docs with IDs 123 or 124
    { -ids => [123,124] }

    # docs of types 'blog' or 'comment' with IDs 123 or 124
    {
        -ids => {
            type    => ['blog','comment'],
            values  => [123,124]

        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/ids-query.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html>

=head2 -type

Filters docs with matching C<_type> fields:

*** For filters only ***

    # Filter docs of type 'comment'
    { -type => 'comment' }

    # Filter docs of type 'comment' or 'blog'
    { -type => ['blog','comment' ]}

See L<http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html>

=head1 PARENT/CHILD

Documents stored in ElasticSearch can be configured to have parent/child
relationships.

See L<http://www.elasticsearch.org/guide/reference/mapping/parent-field.html>
for more.

=head2 has_child | not_has_child

Find parent documents that have child documents which match a query.

    # Find parent docs whose children of type 'comment' have the tag 'perl'
    {
        -has_child => {
            type   => 'comment',
            query  => { tag => 'perl' },
            _scope => 'my_scope',
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/has-child-query.html>
and L<http://www.elasticsearch.org/guide/reference/query-dsl/has-child-filter.html>.

=head2 top_children

The C<top_children> query runs a query against the child docs, and aggregates
the scores to find the parent docs whose children best match.

*** For queries only ***

    {
        -top_children => {
            type                => 'blog_tag',
            query               => { tag => 'perl' },
            score               => 'max',
            factor              => 5,
            incremental_factor  => 2,
            _scope              => 'my_scope'
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/top-children-query.html>

=head1 CACHING FILTERS

Part of the performance boost that you get when using filters comes from the
ability to cache the results of those filters.  However, it doesn't make
sense to cache all filters by default.

If you would like to override the default caching, then you can use
C<-cache> or C<-nocache>:

    # Don't cache the term filter for 'status'
    {
        content => { text => 'interesting post'},
        -filter => {
            -nocache => { status => 'active' }
        }
    }

    # Do cache the numeric range filter:
    {
        content => { text => 'interesting post'},
        -filter => {
            -cache => { created => {'>' => '2010-01-01' } }
        }
    }

See L<http://www.elasticsearch.org/guide/reference/query-dsl/> for more
details about what is cached by default and what is not.


=head1 ELASTICSEARCH CONCEPTS

=head2 Filters vs Queries

ElasticSearch supports filters and queries:

=over

=item *

A filter just answers the question: "Does this field match? Yes/No", eg:

=over

=item *

Does this document have the tag C<"beta">?

=item *

Was this document published in 2011?

=back

=item *

A query is used to calculate relevance (
known in ElasticSearch as C<_score>):

=over

=item *

Give me all documents that include the keywords C<"Foo"> and C<"Bar"> and
rank them in order of relevance.

=item *

Give me all documents whose C<tag> field contains C<"perl"> or C<"ruby">
and rank documents that contain BOTH tags more highly.

=back

=back

Filters are lighter and faster, and the results can often be cached, but they
don't contribute to the C<_score> in any way.

Typically, most of your clauses will be filters, and just a few will be queries.

=head2 Terms vs Text

All data is stored in ElasticSearch as a C<term>, which is an exact value.
The term C<"Foo"> is not the same as C<"foo">.

While this is useful for fields that have discreet values (eg C<"active">,
C<"inactive">), it is not sufficient to support full text search.

ElasticSearch has to I<analyze> text to convert it into terms. This applies
both to the text that the stored document contains, and to the text that the
user tries to search on.

The default analyzer will:

=over

=item *

split the text on (most) punctuation and remove that punctuation

=item *

lowercase each word

=item *

remove English stopwords

=back

For instance, C<"The 2 GREATEST widgets are foo-bar and fizz_buzz"> would result
in the terms C< [2,'greatest','widgets','foo','bar','fizz_buzz']>.

It is important that the same analyzer is used both for the stored text
and for the search terms, otherwise the resulting terms may be different,
and the query won't succeed.

For instance, a C<term> query for C<GREATEST> wouldn't work, but C<greatest>
would work.  However, a C<text> query for C<GREATEST> would work, because
the search text would be analyzed into the correct terms.

See L<http://www.elasticsearch.org/guide/reference/index-modules/analysis/>
for the list of supported analyzers.

=cut

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 BUGS

This is an alpha module, so there will be bugs, and the API is likely to
change in the future.

If you have any suggestions for improvements, or find any bugs, please report
them to L<https://github.com/clintongormley/ElasticSearch-SearchBuilder/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ElasticSearch::SearchBuilder


You can also look for information at: L<http://www.elasticsearch.org>


=head1 ACKNOWLEDGEMENTS

Thanks to SQL::Abstract for providing the inspiration and some of the internals.

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

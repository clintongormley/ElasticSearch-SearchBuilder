package ElasticSearch::SearchBuilder;

use Carp;
use strict;
use warnings;
use Scalar::Util ();

=head1 NAME

ElasticSearch::SearchBuilder - The great new ElasticSearch::SearchBuilder!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use ElasticSearch::SearchBuilder;

    my $foo = ElasticSearch::SearchBuilder->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=cut

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

    my @distributed = map {
        { $k => $_ }
    } @v;
    unshift @distributed, $op
        if $op;

    my $logic = $op ? substr( $op, 1 ) : '';
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
            my $handler = "_{$type}_field_$op";
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
    croak "$k => UNDEF not a supported query";
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
sub _query_unary_top_children { shift->_unary_children( 'query', shift ) }
#===================================

#===================================
sub _filter_unary_or  { shift->_unary_and( 'filter', shift, 'or' ) }
sub _filter_unary_and { shift->_unary_and( 'filter', shift, 'and' ) }
sub _filter_unary_not { shift->_unary_not( 'filter', shift, ) }
sub _filter_unary_ids { shift->_unary_ids( 'filter', shift ) }
sub _filter_unary_has_child { shift->_unary_child( 'filter', shift ) }
sub _filter_unary_top_children { shift->_unary_children( 'filter', shift ) }
#===================================

#===================================
sub _unary_and {
#===================================
    my ( $self, $type, $v, $op ) = @_;
    $self->_SWITCH_refkind(
        $v,
        {   ARRAYREF => sub { return $self->_top_ARRAYREF( $type, $v, $op ) },
            HASHREF => sub {
                return ( $op =~ /^or/i )
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

    return $self->_negate_query($clause);
}

#===================================
sub _unary_ids {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
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
sub _unary_children {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
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
            FALLBACK =>
                sub { croak "$type 'top_children' accepts a hash ref" },
        }
    );
}

#===================================
sub _unary_child {
#===================================
    my ( $self, $type, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   HASHREF => sub {
                my $p
                    = $self->_hash_params( 'has_child', $v,
                    [ 'query', 'type' ],
                    ['_scope'] );
                $p->{query} = $self->_recurse( 'query', $p->{query} );
                return { has_child => $p };
            },
            FALLBACK => sub { croak "$type 'has_child' accepts a hash ref" },
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
        $v,
        {   SCALAR  => sub { return { query_string => { query => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'query_string',
                    $v,
                    ['query'],
                    [   qw(fields default_operator analyzer allow_leading_wildcard
                            lowercase_expanded_terms enable_position_increments
                            fuzzy_prefix_length fuzzy_min_sim phrase_slop boost
                            use_dis_max tie_breaker
                            analyze_wildcard auto_generate_phrase_queries)
                    ]
                );
                return { query_string => $p };
            },
            FALLBACK =>
                sub { croak "'query_string' accepts scalar or a hashref" },
        }
    );
}

#===================================
sub _query_unary_flt {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   SCALAR  => sub { return { flt => { like_text => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'flt', $v,
                    ['like_text'],
                    [   qw(fields ignore_tf max_query_terms
                            min_similarity prefix_length boost)
                    ]
                );
                return { flt => $p };
            },
            FALLBACK => sub { croak "'flt' accepts scalar or a hashref" },
        }
    );
}

#===================================
sub _query_unary_mlt {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   SCALAR  => sub { return { mlt => { like_text => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    'mlt', $v,
                    ['like_text'],
                    [   qw(fields percent_terms_to_match min_term_freq
                            max_query_terms stop_words
                            min_doc_freq max_doc_freq
                            min_word_len max_word_len
                            boost_terms boost)
                    ]
                );
                return { mlt => $p };
            },
            FALLBACK => sub { croak "'mlt' accepts scalar or a hashref" },
        }
    );
}

#===================================
sub _query_unary_custom_score {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   HASHREF => sub {
                my $p
                    = $self->_hash_params( 'custom_score', $v,
                    [ 'query', 'script' ],
                    ['params'] );
                return { custom_score => $p };
            },
            FALLBACK => sub { croak "'custom_score' accepts a hashref" },
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
            FALLBACK => sub {
                croak "'dis_max' accepts scalar, arrayref or a hashref";
            },
        }
    );
}

#===================================
sub _query_unary_boosting {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params( 'dis_max', $v,
                    [ 'positive', 'negative', 'negative_boost' ] );

                $p = $self->_multi_queries( $p, 'positive', 'negative' );
                return { boosting => $p };
            },
            FALLBACK =>
                sub { croak "'boosting' accepts scalar or a hashref" },
        }
    );
}

#===================================
sub _query_unary_bool {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   HASHREF => sub {
                my $p = $self->_hash_params(
                    'boost', $v,
                    [   qw(must should should_not boost
                            minimum_number_should_match max_clause_count)
                    ]
                );
                $p = $self->_multi_queries( $p, 'must', 'should',
                    'should_not' );
                return { bool => $p };
            },
            FALLBACK => sub { croak "'bool' accepts a hashref" },
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
        $v,
        {   SCALAR => sub { return { $op => { field => $v } } },
            FALLBACK => sub { croak "-$op accepts a field name only" },
        }
    );
}

#===================================
sub _filter_unary_type {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   SCALAR   => sub { return { type => { value => $v } } },
            ARRAYREF => sub { return { type => { value => $v } } },
            FALLBACK =>
                sub { croak "-type accepts a scalar or arrayref only" },
        }
    );
}

#===================================
sub _filter_unary_script {
#===================================
    my ( $self, $v ) = @_;
    return $self->_SWITCH_refkind(
        $v,
        {   SCALAR  => sub { return { script => { script => $v } } },
            HASHREF => sub {
                my $p = $self->_hash_params( 'script', $v, ['script'],
                    ['params'] );
                return { script => $p };
            },
            FALLBACK => sub { croak "'script' accepts scalar or a hashref" },
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
        $v,
        {   ARRAYREF => sub { $self->_recurse( 'filter', $v ) },
            HASHREF  => sub { $self->_recurse( 'filter', $v ) },
            FALLBACK =>
                sub { croak "-$op accepts a hashref or arrayref only" },
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
    shift->_query_field_generic( @_, ['value'], ['boost'] );
}

#===================================
sub _query_field_wildcard {
#===================================
    shift->_query_field_generic( @_, ['value'], ['boost'] );
}

#===================================
sub _query_field_fuzzy {
#===================================
    shift->_query_field_generic( @_, ['value'],
        [qw(boost min_similarity max_expansions prefix_length)] );
}

#===================================
sub _query_field_text {
#===================================
    shift->_query_field_generic( @_, ['query'],
        [qw(operator analyzer fuzziness max_expansions prefix_length)] );
}

#===================================
sub _query_field_phrase {
#===================================
    shift->_query_field_generic( @_, ['query'], ['slop'] );
}

#===================================
sub _query_field_phrase_prefix {
#===================================
    shift->_query_field_generic( @_, ['query'], ['max_expansions'] );
}

#===================================
sub _query_field_field {
#===================================
    shift->_query_field_generic(
        @_,
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
    my ( $self, $k, $op, $val, $req, $opt ) = @_;

    return $self->_SWITCH_refkind(
        $val,
        {   SCALAR   => sub { return { $op => { $k => $val } } },
            ARRAYREF => sub {
                my $method = "_query_field_${op}";
                my @queries = map { $self->$method( $k, $op, $_ ) } @$val;
                return $self->_join_clauses( 'query', 'or', \@queries ),;
            },
            HASHREF => sub {
                my $p = $self->_hash_params( $op, $val, $req, $opt );
                return { $op => { $k => $p } };
            },
            FALLBACK => sub { croak "'$op' accepts a scalar or array ref" },
        }
    );
}

#===================================
sub _query_field_terms {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
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
                return { term => $p };
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
                return $self->_join_clauses( 'query', 'and', @queries );
            },

        },
    );
}

#===================================
sub _query_field_mlt {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        $val,
        {   SCALAR => sub {
                return { mlt_field => { $k => { like_text => $val } } };
            },
            ARRAYREF => sub {
                my $method = "_query_field_${op}";
                my @queries
                    = map { $self->_query_field_mlt( $k, $op, $_ ) } @$val;
                return $self->_join_clauses( 'query', 'or', \@queries ),;
            },
            HASHREF => sub {
                my $p = $self->_hash_params(
                    $op, $val,
                    ['like_text'],
                    [   qw(percent_terms_to_match min_term_freq max_query_terms
                            stop_words min_doc_freq max_doc_freq min_word_len
                            max_word_len boost_terms boost )
                    ]
                );
                return { mlt_field => { $k => $p } };
            },
            FALLBACK => sub { croak "'$op' accepts a scalar or array ref" },
        }
    );
}

#===================================
sub _query_field_range {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    my $es_op = $RANGE_MAP{$op} || $op;

    return $self->_SWITCH_refkind(
        $val,
        {   HASHREF => sub {

            },
            SCALAR => sub {
                return { 'range' => { $k => { $es_op => $val } } };
            },
            FALLBACK => sub { croak "'$op' accepts a scalar value" },
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
                return $self->_join_clauses( 'filter', 'and', @filters );
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
        $val,
        {   SCALAR => sub {
                return { $type => { $k => { $es_op => $val } } };
            },
            FALLBACK => sub { croak "'$op' accepts a scalar value" },
        }
    );
}

#===================================
sub _filter_field_prefix {
#===================================
    my ( $self, $k, $op, $val ) = @_;

    return $self->_SWITCH_refkind(
        $val,
        {   SCALAR   => sub { return { prefix => { $k => $val } } },
            ARRAYREF => sub {
                my @filters
                    = map { $self->_filter_field_prefix( $k, $op, $_ ) }
                    @$val;
                return $self->_join_clauses( 'filter', 'or', \@filters ),;
            },
            FALLBACK =>
                sub { croak "'prefix' accepts a scalar or array ref" },
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
        $val,
        {   SCALAR => sub {
                if ( $op eq 'missing' ) { $val = !$val }
                return { ( $val ? 'exists' : 'missing' ) => { field => $k } };
            },
            FALLBACK => sub { croak "'$op' accepts only a scalar" },
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
    my $p    = $self->_hash_params( @_, [qw(from to location)] );
    return {
        geo_distance_range => {
            from => $p->{from},
            to   => $p->{to},
            $k   => $p->{location}
        }
    };
}

#===================================
sub _filter_field_geo_polygon {
#===================================
    my $self = shift;
    my $k    = shift;
    my $p    = $self->_hash_params( @_, [qw(points)] );
    return { geo_polygon => { $k => $p->{points} } };
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
    my ( $self, $data, $dispatch_table ) = @_;

    my $coderef;
    for ( @{ $self->_try_refkind($data) } ) {
        $coderef = $dispatch_table->{$_}
            and last;
    }
    croak "no dispatch entry for " . $self->_refkind($data)
        unless $coderef;

    my $val = $coderef->();
    return $val;
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
        $params->{$key} = $self->_recurse( 'query', $v );
    }
    return $params;
}

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-elasticsearch-SearchBuilder
at rt.cpan.org>, or through
the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=ElasticSearch-SearchBuilder>.
I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc ElasticSearch::SearchBuilder


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=ElasticSearch-SearchBuilder>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/ElasticSearch-SearchBuilder>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/ElasticSearch-SearchBuilder>

=item * Search CPAN

L<http://search.cpan.org/dist/ElasticSearch-SearchBuilder/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;    # End of ElasticSearch::SearchBuilder

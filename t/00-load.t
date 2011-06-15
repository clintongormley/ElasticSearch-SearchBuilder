#!perl

our $test_num;
BEGIN { $test_num = 306 }

use Test::Most qw(defer_plan);

BEGIN {
    use_ok('ElasticSearch::SearchBuilder') || print "Bail out!";
}

diag(
    "Testing ElasticSearch::SearchBuilder $ElasticSearch::SearchBuilder::VERSION, Perl $], $^X"
);

my $a;
ok $a = ElasticSearch::SearchBuilder->new, 'Instantiate';

my @filter_tests = (

    # TERM
    'K => V',
    { k    => 'v' },
    { term => { k => 'v' } },

    'K => { == V}',
    { k    => { '==' => 'v' } },
    { term => { k    => 'v' } },

    'K => { = V}',
    { k    => { '=' => 'v' } },
    { term => { k   => 'v' } },

    # RANGE
    'K => { gt V}',
    { k     => { 'gt' => 'v' } },
    { range => { k    => { 'gt' => 'v' } } },

    'K => { gte V}',
    { k     => { 'gte' => 'v' } },
    { range => { k     => { 'gte' => 'v' } } },

    'K => { lt V}',
    { k     => { 'lt' => 'v' } },
    { range => { k    => { 'lt' => 'v' } } },

    'K => { lte V}',
    { k     => { 'lte' => 'v' } },
    { range => { k     => { 'lte' => 'v' } } },

    # NUMERIC RANGE
    'K => { > V}',
    { k             => { '>' => 'v' } },
    { numeric_range => { k   => { 'gt' => 'v' } } },

    'K => { >= V}',
    { k             => { '>=' => 'v' } },
    { numeric_range => { k    => { 'gte' => 'v' } } },

    'K => { < V}',
    { k             => { '<' => 'v' } },
    { numeric_range => { k   => { 'lt' => 'v' } } },

    'K => { <= V}',
    { k             => { '<=' => 'v' } },
    { numeric_range => { k    => { 'lte' => 'v' } } },

    # EXISTS
    '-exists => K',
    { -exists => 'k' },
    { exists => { field => 'k' } },

    'K => {exists 1}}',
    { k      => { exists => 1 } },
    { exists => { field  => 'k' } },

    # MISSING
    'K => undef',
    { k       => undef },
    { missing => { field => 'k' } },

    '-missing => K',
    { -missing => 'k' },
    { missing => { field => 'k' } },

    'K => {missing 1}}',
    { k       => { -missing => 1 } },
    { missing => { field   => 'k' } },

    # IN
    'K => { in @vals}',
    { k => { 'in'    => [ 'v1', 'v2' ] } },
    { terms => { k => [ 'v1', 'v2' ] } },


    # IN/NOT_IN/==/!=
    'K => @V',
    { k => ['v1','v2']},
    { terms=>{k=>['v1','v2']}},

    'K => -and,@V',
    ['-and'=>[{k=>'v1'},{k=>'v2'}]],
    { and => [{term=>{k=>'v1'}},{term=>{k=>'v2'}}]},

    'K => { == V}',
    { k => {'==' => 'v'}},
    { term => { k => 'v'}},

    'K => { == @V}',
    { k => {'==' => ['v1','v2']}},
    { terms=>{k=>['v1','v2']}},

    'K => { != V} ',
    {k => {'!=' => 'v'}},
    { not =>{term => {k => 'v'}}},

    'K => { != @V}',
    { k => {'!='=>['v1','v2']}},
    { not => {terms=>{k=>['v1','v2']}}},


);

while (@filter_tests) {
    test_filter( splice( @filter_tests, 0, 3 ) );
}

all_done;

#===================================
sub test_filter {
#===================================
    my ( $name, $in, $out ) = @_;
    eq_or_diff $a->filter($in), { filter => $out }, $name;
}
